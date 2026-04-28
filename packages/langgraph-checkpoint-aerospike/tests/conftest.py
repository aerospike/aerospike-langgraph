import contextlib
import os

import aerospike
import aerospike.exception
import pytest

# Default sets `AerospikeSaver` writes to. Tests that use the shared `saver`
# fixture target these; tests that override the set names (e.g.
# `test_fanout_aerospike`) must clean up their own sets.
_DEFAULT_CHECKPOINT_SETS = ("lg_cp", "lg_cp_w", "lg_cp_meta")


def _connect_client():
    host = os.getenv("AEROSPIKE_HOST", "localhost")
    port = int(os.getenv("AEROSPIKE_PORT", "3000"))
    cfg = {"hosts": [(host, port)]}
    return aerospike.client(cfg).connect()


@pytest.fixture(scope="session")
def aerospike_namespace():
    return os.getenv("AEROSPIKE_NAMESPACE", "test")


@pytest.fixture(scope="session")
def client():
    try:
        c = _connect_client()
    except aerospike.exception.AerospikeError as e:
        pytest.skip(f"Could not connect to Aerospike: {e}")
    yield c
    with contextlib.suppress(Exception):
        c.close()


@pytest.fixture(scope="session")
def aerospike_saver_cls():
    from langgraph.checkpoint.aerospike import AerospikeSaver

    return AerospikeSaver


@pytest.fixture()
def truncate_sets(client, aerospike_namespace):
    """Return a callable that truncates the given Aerospike sets.

    Used by per-test fixtures to wipe state before/after each test.
    Truncate is the idiomatic Aerospike way: a single server-side op per
    set, LUT-filtered so any record written *after* the call is preserved.
    Secondary indexes (e.g. AerospikeSaver's `thread_id` index) survive
    truncate, so we don't need to recreate them.
    """

    def _do(sets):
        for s in sets:
            with contextlib.suppress(aerospike.exception.AerospikeError):
                client.truncate(aerospike_namespace, s, 0)

    return _do


def _test_ttl_config() -> dict:
    """TTL configuration for `AerospikeSaver` instances in tests.

    Tweak via env vars without touching code:
        TEST_DEFAULT_TTL_MINUTES  (default 60)
        TEST_REFRESH_ON_READ      (default true)
    """
    default_ttl = int(os.getenv("TEST_DEFAULT_TTL_MINUTES", "60"))
    refresh = os.getenv("TEST_REFRESH_ON_READ", "true").lower() == "true"
    return {
        "default_ttl": default_ttl,
        "refresh_on_read": refresh,
    }


@pytest.fixture()
def saver(aerospike_saver_cls, client, aerospike_namespace, truncate_sets):
    """Yield a fresh `AerospikeSaver` with the default sets pre-truncated."""
    truncate_sets(_DEFAULT_CHECKPOINT_SETS)
    s = aerospike_saver_cls(
        client=client,
        namespace=aerospike_namespace,
        ttl=_test_ttl_config(),
    )
    try:
        yield s
    finally:
        truncate_sets(_DEFAULT_CHECKPOINT_SETS)


@pytest.fixture()
def cfg_base():
    return {"configurable": {"thread_id": "1", "checkpoint_ns": "demo", "user_id": "jagrut"}}
