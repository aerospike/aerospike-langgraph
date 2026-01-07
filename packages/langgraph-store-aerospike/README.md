# LangGraph Store Aerospike

Store LangGraph state and data in Aerospike using the provided `AerospikeStore`.

## Installation

```bash
pip install -U langgraph-store-aerospike
```

## Usage

1. Bring up Aerospike locally using prebuilt [Aerospike Docker Image](https://hub.docker.com/_/aerospike):

```bash
docker run -d --name aerospike -p 3000-3002:3000-3002 container.aerospike.com/aerospike/aerospike-server
```

2. Point the store at your cluster (Default):
   - `AEROSPIKE_HOST=127.0.0.1`
   - `AEROSPIKE_PORT=3000`
   - `AEROSPIKE_NAMESPACE=langgraph` (default namespace for the store)
   - `AEROSPIKE_SET=store` (default set name)

3. Use in your application:

```python
   import aerospike
   from langgraph.store.aerospike import AerospikeStore
   from langgraph.store.base import PutOp, GetOp, SearchOp

   client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()

   store = AerospikeStore(
       client=client,
       namespace="test",
       set="langgraph_store"
   )
```
