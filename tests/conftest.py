# tests/conftest.py
import os
import pytest

def _connect_client():
    import aerospike
    host = os.getenv("AEROSPIKE_HOST", "127.0.0.1")
    port = int(os.getenv("AEROSPIKE_PORT", "3000"))
    cfg = {"hosts": [(host, port)]}
    client = aerospike.client(cfg).connect()
    return client

@pytest.fixture(scope="session")
def aerospike_namespace():
    return os.getenv("AEROSPIKE_NAMESPACE", "test")

@pytest.fixture(scope="session")
def client():
    import aerospike
    try:
        c = _connect_client()
    except aerospike.exception.AerospikeError as e:
        pytest.skip(f"Could not connect to Aerospike: {e}")
    yield c
    try:
        c.close()
    except Exception:
        pass

@pytest.fixture(scope="session")
def AerospikeSaver():
    # Adjust import if your class lives at a different path
    from langgraph.checkpoint.aerospike import AerospikeSaver
    return AerospikeSaver

@pytest.fixture()
def saver(AerospikeSaver, client, aerospike_namespace):
    return AerospikeSaver(client=client, namespace=aerospike_namespace)

@pytest.fixture()
def cfg_base():
    # Reuse this in tests; use any namespace/thread you like
    return {"configurable": {"thread_id": "1", "checkpoint_ns": "demo", "user_id": "jagrut"}}
