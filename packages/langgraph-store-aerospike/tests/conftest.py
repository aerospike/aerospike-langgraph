import contextlib
import os

import aerospike
import aerospike.exception
import pytest

# Single shared set name. Tests that need isolation get it from the
# `truncate_sets` fixture clearing this set before/after every test.
_STORE_SET = "store_test"


@pytest.fixture(scope="session")
def client():
    """Single Aerospike client shared across the whole test session."""
    host = os.getenv("AEROSPIKE_HOST", "127.0.0.1")
    port = int(os.getenv("AEROSPIKE_PORT", "3000"))
    cfg = {"hosts": [(host, port)]}
    try:
        c = aerospike.client(cfg).connect()
    except aerospike.exception.AerospikeError as e:
        pytest.skip(f"Could not connect to Aerospike at {host}:{port}: {e}")
    yield c
    with contextlib.suppress(Exception):
        c.close()


@pytest.fixture(scope="session")
def namespace():
    """Aerospike namespace used for tests (matches Docker default)."""
    return os.getenv("AEROSPIKE_NAMESPACE", "test")


@pytest.fixture()
def truncate_sets(client, namespace):
    """Return a callable that truncates the given Aerospike sets.

    Truncate is the idiomatic Aerospike way to wipe state: a single
    server-side op per set, LUT-filtered so any record written *after* the
    call is preserved. Cheap to call before/after every test.
    """

    def _do(sets):
        for s in sets:
            with contextlib.suppress(aerospike.exception.AerospikeError):
                client.truncate(namespace, s, 0)

    return _do


@pytest.fixture()
def store(client, namespace, truncate_sets):
    """Yield a freshly-truncated `AerospikeStore` for each test."""
    from langgraph.store.aerospike.store import AerospikeStore

    truncate_sets((_STORE_SET,))
    try:
        yield AerospikeStore(client=client, namespace=namespace, set=_STORE_SET)
    finally:
        truncate_sets((_STORE_SET,))
