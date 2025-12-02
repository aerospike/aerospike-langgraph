from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from collections.abc import Iterator

from langchain_core.runnables import RunnableConfig

import aerospike
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    CheckpointTuple,
    ChannelVersions,
    WRITES_IDX_MAP
)

SEP = "|"


def _now_ns() -> int:
    return time.time_ns()


class AerospikeSaver(BaseCheckpointSaver):
    """
    Minimal checkpointer with zero server-side querying requirements.

    Public API expects a RunnableConfig-like dict:

        config = {
            "configurable": {
                "thread_id": "<required>",
                "checkpoint_ns": "<required>",
                # optional:
                "checkpoint_id": "<for put/get or explicit resume>",
                "before": "<for list()>",
            },
            # you can also pass tags/metadata/etc. but they're ignored here
        }

    Storage layout (all in a single namespace):
      - main records (set=self.set_cp):
            key:  "{thread_id}|{checkpoint_ns}|{checkpoint_id}"
            bins: {
                "thread_id": str,
                "checkpoint_ns": str,
                "checkpoint_id": str,
                "checkpoint": str (JSON),
                "metadata":   str (JSON),
                "ts":         int (ns since epoch),
            }

      - latest pointer record (set=self.set_meta):
            key:  "{thread_id}|{checkpoint_ns}|__latest__"
            bins: { "checkpoint_id": str, "ts": int }

      - timeline record (set=self.set_meta):
            key:  "{thread_id}|{checkpoint_ns}|__timeline__"
            bins: { "items": str(JSON list[[ts:int, checkpoint_id:str], ...]) }

      - writes (optional, set=self.set_writes):
            key:  "{thread_id}|{checkpoint_ns}|{checkpoint_id}"
            bins: { "writes": str(JSON) }
    """

    def __init__(
        self,
        client: aerospike.Client,
        namespace: str = "test",
        set_cp: str = "lg_cp",
        set_writes: str = "lg_cp_w",
        set_meta: str = "lg_cp_meta",
        ttl: Optional[int] = None,
        timeline_max: int = 500,
    ) -> None:
        self.client = client
        self.ns = namespace
        self.set_cp = set_cp
        self.set_writes = set_writes
        self.set_meta = set_meta
        self.ttl = ttl
        self.timeline_max = max(1, int(timeline_max))

    # ---------- config parsing ----------
    @staticmethod
    def _ids_from_config(config: Optional[Dict[str, Any]]) -> Tuple[str, str, Optional[str], Optional[str]]:
        """
        Returns (thread_id, checkpoint_ns, checkpoint_id, before)
        """
        cfg = (config or {})
        c = cfg.get("configurable", {}) or {}
        md = cfg.get("metadata", {}) or {}

        thread_id = c.get("thread_id") or md.get("thread_id")
        if not thread_id:
            raise ValueError("configurable.thread_id is required in RunnableConfig")

        checkpoint_ns = (
            c.get("checkpoint_ns")
            or md.get("checkpoint_ns")
            or ""
        )

        checkpoint_id = c.get("checkpoint_id")

        return thread_id, checkpoint_ns, checkpoint_id

    # ---------- keys ----------
    def _key_cp(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str):
        return (self.ns, self.set_cp, f"{thread_id}{SEP}{checkpoint_ns}{SEP}{checkpoint_id}")

    def _key_writes(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str):
        return (self.ns, self.set_writes, f"{thread_id}{SEP}{checkpoint_ns}{SEP}{checkpoint_id}")

    def _key_latest(self, thread_id: str, checkpoint_ns: str):
        return (self.ns, self.set_meta, f"{thread_id}{SEP}{checkpoint_ns}{SEP}__latest__")

    def _key_timeline(self, thread_id: str, checkpoint_ns: str):
        return (self.ns, self.set_meta, f"{thread_id}{SEP}{checkpoint_ns}{SEP}__timeline__")

    # ---------- aerospike io ----------
    def _put(self, key, bins: Dict[str, Any]) -> None:
        meta = {"ttl": self.ttl} if self.ttl is not None else None
        try:
            self.client.put(key, bins, meta)
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike put failed for {key}: {e}") from e

    def _get(self, key) -> Optional[Tuple]:
        try:
            return self.client.get(key)
        except aerospike.exception.RecordNotFound:
            return None
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike get failed for {key}: {e}") from e

    def _read_timeline_items(self, timeline_key) -> List[Tuple[int, str]]:
        rec = self._get(timeline_key)
        if rec is None:
            return []
        bins = rec[2]
        try:
            items = json.loads(bins.get("items", "[]"))
            cleaned: List[Tuple[int, str]] = []
            for it in items:
                if isinstance(it, list) and len(it) == 2 and isinstance(it[1], str):
                    cleaned.append((it[0], it[1]))
            return cleaned
        except Exception:
            return []

    # ---------- public API (RunnableConfig-based) ----------
    def put(
        self,
        config: Dict[str, Any],
        checkpoint: Dict[str, Any],
        metadata: Dict[str, Any],
        new_versions: ChannelVersions,
    ) -> Dict[str, Any]:
            
        """
        Save/overwrite a checkpoint and advance latest/timeline pointers.

        LangGraph will pass in:
          - config: current RunnableConfig (may or may not have checkpoint_id set)
          - checkpoint: full checkpoint dict (includes 'id' and 'ts')
          - metadata: CheckpointMetadata dict
          - new_versions: channel versions updated in this step (we ignore for now)
        """
        thread_id, checkpoint_ns, parent_checkpoint_id = self._ids_from_config(config)
        checkpoint_id = checkpoint.get("id")
        if not checkpoint_id:
            raise ValueError("checkpoint_id is required for put()")

        ts = checkpoint.get("ts")
        if ts is None:
            ts = _now_ns()
            checkpoint["ts"] = ts

        cp_type, cp_bytes = self.serde.dumps_typed(checkpoint)
        metadata = metadata.copy()
        metadata.update(config.get("metadata", {}))

        meta_type, meta_bytes = self.serde.dumps_typed(metadata)


        key = self._key_cp(thread_id, checkpoint_ns, checkpoint_id)
        rec = {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "p_checkpoint_id": parent_checkpoint_id,
            "cp_type": cp_type,
            "checkpoint": cp_bytes,
            "meta_type": meta_type,
            "metadata": meta_bytes,
            "ts": ts,
        }
        self._put(key, rec)

        latest_key = self._key_latest(thread_id, checkpoint_ns)
        self._put(latest_key, {"checkpoint_id": checkpoint_id, "ts": ts})

        timeline_key = self._key_timeline(thread_id, checkpoint_ns)
        items = self._read_timeline_items(timeline_key)

        items = [(t, cid) for (t, cid) in items if cid != checkpoint_id]
        items.insert(0, (ts, checkpoint_id))
        if len(items) > self.timeline_max:
            items = items[: self.timeline_max]
        self._put(timeline_key, {"items": json.dumps(items)})

        new_config = dict(config)
        cfg_conf = dict(new_config.get("configurable") or {})
        cfg_conf.update(
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        )
        new_config["configurable"] = cfg_conf
        return new_config


    def put_writes(
        self,
        config: Dict[str, Any],
        writes: Iterable[Dict[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Persist per-checkpoint write-set (optional).
        LangGraph will call this with:
          - config: RunnableConfig (with checkpoint_id)
          - writes: iterable of write records
          - task_id / task_path: identifiers for the task (we ignore them)
        """
        if not writes:
            return

        thread_id, checkpoint_ns, checkpoint_id = self._ids_from_config(config)
        if not checkpoint_id:
            return
        
        key = self._key_writes(thread_id, checkpoint_ns, checkpoint_id) # Do we need to put task id as key. (Reason: Record limit 8Mb)

        existing_rec = self._get(key)
        existing_items: List[Dict[str, Any]] = []
        if existing_rec is not None:
            _, _, bins = existing_rec
            existing_items = bins.get("writes")
        
        now_ts = _now_ns()

        for idx, (channel, value) in enumerate(writes):
            idx_val = WRITES_IDX_MAP.get(channel, idx)
            type_, serialized = self.serde.dumps_typed(value)

            new_item = {
                "task_id": task_id,
                "task_path": task_path,
                "channel": channel,
                "idx": idx_val,
                "type": type_,
                "value": serialized,
                "ts": now_ts,
            }
            replace_at: Optional[int] = None
            for i, item in enumerate(existing_items):
                if item.get("task_id") == task_id and item.get("idx") == idx_val:
                    replace_at = i
                    break

            if replace_at is not None:
                existing_items[replace_at] = new_item
            else:
                existing_items.append(new_item)

        
        self._put(key, {"writes": existing_items})

    def get_tuple(
        self,
        config: Dict[str, Any],
    ) -> Optional[CheckpointTuple]:
        """
        If configurable.checkpoint_id is omitted, returns the latest.
        """
        thread_id, checkpoint_ns, checkpoint_id = self._ids_from_config(config)

        if checkpoint_id is None:
            latest = self._get(self._key_latest(thread_id, checkpoint_ns))
            if latest is None or "checkpoint_id" not in latest[2]:
                return None
            checkpoint_id = latest[2]["checkpoint_id"]

        key = self._key_cp(thread_id, checkpoint_ns, checkpoint_id)
        got = self._get(key)
        if got is None:
            return None

        _, _, bins = got

        cp_type = bins.get("cp_type")
        raw_cp = bins.get("checkpoint")
        raw_meta = bins.get("metadata")
        meta_type = bins.get("meta_type")
        if cp_type is None or raw_cp is None:
            return None
        try:
            checkpoint = self.serde.loads_typed((cp_type, raw_cp))
        except Exception:
            return None
        
        if meta_type is None or raw_meta is None:
            return None
        try:
            metadata = self.serde.loads_typed((meta_type, raw_meta))
        except Exception:
            return None
        
        pending_writes: List[Tuple[str, str, Any]] = []
        wrec = self._get(self._key_writes(thread_id, checkpoint_ns, checkpoint_id))
        if wrec is not None:
            _, _, wbins = wrec
            items = wbins.get("writes") or []
            for item in items:
                try:
                    task_id = item.get("task_id", "")
                    channel = item["channel"]
                    type_ = item["type"]
                    serialized = item["value"]
                    value = self.serde.loads_typed((type_, serialized))
                    pending_writes.append((task_id, channel, value))
                except KeyError:
                    continue

        cp_config: Dict[str, Any] = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

        return CheckpointTuple(
            config=cp_config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": bins.get("p_checkpoint_id"),
                    }
                }
                if bins.get("p_checkpoint_id")
                else None
            ),
            pending_writes=pending_writes,
        )

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints that match the given criteria.

        Args:
            config: Base configuration for filtering checkpoints.
            filter: Additional filtering criteria.
            before: List checkpoints created before this configuration.
            limit: Maximum number of checkpoints to return.

        Returns:
            Iterator of matching checkpoint tuples.

        Raises:
            NotImplementedError: Implement this method in your custom checkpoint saver.
        """
        thread_id, checkpoint_ns, _ = self._ids_from_config(config or {})

        timeline_key = self._key_timeline(thread_id, checkpoint_ns)
        items = self._read_timeline_items(timeline_key)

        before_id: Optional[str] = None
        if before is not None:
            _, _, before_id = self._ids_from_config(before or {})

        if before_id:
            seen = False
            new_items: List[Tuple[int, str]] = []
            for ts, cid in items:
                if not seen:
                    if cid == before_id:
                        seen = True
                    continue
                new_items.append((ts, cid))
            items = new_items

        yielded = 0
        for _, cid in items:
            if limit is not None and yielded >= limit:
                break

            cp_config: Dict[str, Any] = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": cid,
                }
            }

            tpl = self.get_tuple(cp_config)
            if tpl is None:
                continue

            if filter:
                ok = True
                for k, v in filter.items():
                    if tpl.metadata.get(k) != v:
                        ok = False
                        break
                if not ok:
                    continue

            yielded += 1
            yield tpl
