# LangGraph Aerospike Checkpointer

Store LangGraph checkpoints in Aerospike using the provided `AerospikeSaver`. The repo includes a minimal Aerospike docker setup, examples, and pytest-based checks.

## Quick start
1) Install deps (Python 3.10+/aerospike client):
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt 
   ```
2) Bring up Aerospike locally:
   ```bash
   docker compose up -d
   ```
3) Point the saver at your cluster (defaults match compose):
   - `AEROSPIKE_HOST=127.0.0.1`
   - `AEROSPIKE_PORT=3000`
   - `AEROSPIKE_NAMESPACE=test`
4) Use in a graph:
   ```python
   import aerospike
   from langgraph.checkpoint.aerospike import AerospikeSaver

   client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()
   saver = AerospikeSaver(client=client, namespace="test")

   compiled = graph.compile(checkpointer=saver)  # graph is your LangGraph graph
   compiled.invoke({"input": "hello"}, config={"configurable": {"thread_id": "demo"}})
   ```

## Where the saver lives
- Core implementation: `langgraph/checkpoint/aerospike/saver.py`.

## Configuration
- Reads `AEROSPIKE_HOST`, `AEROSPIKE_PORT`, `AEROSPIKE_NAMESPACE`; defaults align with `docker-compose.yml`.
- Create a `.env` to override locally (not committed).
- For custom Aerospike configs, adjust `docker-compose.yml`; volume mounts are commented as examples.

## Tests
- Require a reachable Aerospike (`docker compose up -d` is enough).
- Coverage:
  - `tests/test_fanout_aerospike.py`: fanout graph saves/loads checkpoints, custom properties.
  - `tests/test_weather_graph_live.py`: weather graph live flow with Aerospike checkpointer.
  - `tests/test_customer_support_graph_live.py`: Langraph tutorial airline customer support bot(requies Ollama downloaded).
  - `tests/test_graph_smoke_live.py`: smoke coverage for graph compilation/invoke with Aerospike.
  - `tests/test_debug_dump_checkpoint.py`: debug dump helper behavior.
- Run everything:
  ```bash
  pytest
  ```

## Utilities
- `tests/test_debug_dump_checkpoint.py`: takes a thread_id input and outputs the latest decoded checkpoint

## Notes
- If you just want to use the Aerospike checkpointer outside Docker use this command:
   ```bash
   pip install git+https://github.com/Aerospike-langgraph/checkpointer.git
   ```
## Langgraph Aerospike Simple Flow:
- Below is a simple flow of internal functions called by langgraph during checkpointing.
![LangGraph Aerospike Checkpointer Flow](./assets/Langgraph-Aeropsike-Flow.png)

## Summary
