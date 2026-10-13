"""Solis forwarding capture (migration 26, api/solis_forward.py): the message
parser keeps every body and reads time / sn / isRealtime / inverter count the
way the integration guide describes them. No network, no database.
Run with `python -m pytest tests` or plainly `python tests/test_solis_forward.py`.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.solis_forward import parse_forward_message  # noqa: E402

SAMPLE = {
    "time": 1791903646256, "sn": "7A12560109400632", "isRealtime": 0,
    "collector1": {"sn": "7A12560109400632", "model": "WL", "dataUploadCycle": 300, "signal": 85},
    "inverter1": [
        {"sn": "1031A1234567", "inverterModel": "S6-EH1P5K-L", "pac": 3120.5, "eToday": 12.4, "eTotal": 8812.1,
         "batteryPower": -850.0, "batteryCapacitySoc": 64, "psum": -210.0, "familyLoadPower": 2060.5, "state": 1},
        {"sn": "1031A7654321", "inverterModel": "S6-GR1P6K", "pac": 2200.0, "eToday": 9.1, "eTotal": 4411.0, "state": 1},
    ],
}


def test_parses_the_guide_shape():
    row = parse_forward_message(json.dumps(SAMPLE).encode(), "solviva/solis/forward")
    assert row["collector_sn"] == "7A12560109400632"
    assert row["is_realtime"] is True                      # isRealtime = 0 means live
    assert row["inverter_count"] == 2
    assert row["device_time"] == datetime(2026, 10, 13, 15, 0, 46, 256000, tzinfo=timezone.utc)
    assert row["topic"] == "solviva/solis/forward" and row["bytes"] == len(json.dumps(SAMPLE))
    assert row["payload"]["inverter1"][0]["batteryPower"] == -850.0   # verbatim


def test_historical_flag_and_second_timestamps():
    body = dict(SAMPLE, isRealtime=1, time=1791903646)             # seconds, not ms
    row = parse_forward_message(json.dumps(body).encode(), "t")
    assert row["is_realtime"] is False
    assert row["device_time"] == datetime(2026, 10, 13, 15, 0, 46, tzinfo=timezone.utc)


def test_unexpected_bodies_are_kept_not_dropped():
    row = parse_forward_message(b"not json at all", "t")
    assert row["payload"] == {"_raw": "not json at all"} and row["collector_sn"] is None
    row = parse_forward_message(json.dumps([1, 2, 3]).encode(), "t")
    assert row["payload"] == {"_raw": [1, 2, 3]}
    row = parse_forward_message(json.dumps({"sn": "", "time": "garbage"}).encode(), "t")
    assert row["collector_sn"] is None and row["device_time"] is None and row["is_realtime"] is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
