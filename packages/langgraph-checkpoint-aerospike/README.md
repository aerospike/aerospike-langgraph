# LangGraph Checkpoint Aerospike

Store LangGraph checkpoints in Aerospike using the provided `AerospikeSaver`.

## Installation

```bash
pip install -U langgraph-checkpoint-aerospike
```

## Usage

1. Bring up Aerospike locally using prebuilt [Aerospike Docker Image](https://hub.docker.com/_/aerospike):

```bash
docker run -d --name aerospike -p 3000-3002:3000-3002 container.aerospike.com/aerospike/aerospike-server
```

2. Point the saver at your cluster (Default):
   - `AEROSPIKE_HOST=127.0.0.1`
   - `AEROSPIKE_PORT=3000`
   - `AEROSPIKE_NAMESPACE=test`

3. Use in workflow:

   ```python
   import aerospike
   from langgraph.checkpoint.aerospike import AerospikeSaver

   client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()
   saver = AerospikeSaver(client=client, namespace="test")

   compiled = graph.compile(checkpointer=saver)  # graph is your LangGraph graph
   compiled.invoke({"input": "hello"}, config={"configurable": {"thread_id": "demo"}})
   ```
