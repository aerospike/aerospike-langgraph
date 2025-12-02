# tests/test_debug_dump_checkpoint.py
from copy import deepcopy
from pprint import pprint

def test_dump_latest_weather_checkpoint(saver, cfg_base):
    cfg = deepcopy(cfg_base)
    cfg["configurable"]["checkpoint_ns"] = "weather-demo"

    cp = saver.get_tuple(cfg)
    assert cp is not None

    print("\n=== DECODED CHECKPOINT ===")
    pprint(cp.checkpoint)
    print("\n=== METADATA ===")
    pprint(cp.metadata)
