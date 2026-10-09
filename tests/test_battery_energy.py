"""Battery energy from Solis's five-minute curve (migration 24).

Pins the sign convention — batteryPower POSITIVE = charging, NEGATIVE =
discharging (verified live 2026-10-09) — and that the hourly backfill sums the
same fields the database roll-up does. Run with `python -m pytest tests` or
plainly `python tests/test_battery_energy.py`.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.sync_five_minutes_to_supabase import _build_row, BATTERY_ENERGY_KEYS  # noqa: E402
from api.backfill_worker import _hourly_rows, HOUR_SUM_FIELDS  # noqa: E402
from api.app_routes import HOURLY_FIELDS  # noqa: E402

PHT = timezone(timedelta(hours=8))
SYSTEM = {"id": "11111111-1111-1111-1111-111111111111", "user_id": "22222222-2222-2222-2222-222222222222"}


def _point(ts: datetime, battery_w, power_w=0, load_w=0, psum_w=0) -> dict:
    return {"time": int(ts.timestamp() * 1000), "power": power_w, "familyLoadPower": load_w,
            "bypassLoadPower": 0, "psum": psum_w, "batteryPower": battery_w, "batteryCapacitySoc": 50}


def test_discharging_is_negative_battery_power():
    ts = datetime(2026, 10, 9, 23, 0, tzinfo=PHT)
    _, row = _build_row(SYSTEM["user_id"], SYSTEM["id"], _point(ts, battery_w=-1200))
    assert row["battery_status"] == "discharging"
    assert row["battery_discharge_kwh"] == 0.1          # 1200 W × 5/60 h / 1000
    assert row["battery_charge_kwh"] == 0.0


def test_charging_is_positive_battery_power():
    ts = datetime(2026, 10, 9, 12, 0, tzinfo=PHT)
    _, row = _build_row(SYSTEM["user_id"], SYSTEM["id"], _point(ts, battery_w=2400))
    assert row["battery_status"] == "charging"
    assert row["battery_charge_kwh"] == 0.2
    assert row["battery_discharge_kwh"] == 0.0


def test_missing_or_blank_battery_power_is_zero_not_null():
    ts = datetime(2026, 10, 9, 12, 0, tzinfo=PHT)
    for p in ({"time": int(ts.timestamp() * 1000), "power": 100},
              _point(ts, battery_w=""), _point(ts, battery_w="--"), _point(ts, battery_w=None)):
        _, row = _build_row(SYSTEM["user_id"], SYSTEM["id"], p)
        assert row["battery_charge_kwh"] == 0.0 and row["battery_discharge_kwh"] == 0.0


def test_hourly_backfill_sums_battery_like_the_rollup():
    hour = datetime(2026, 10, 9, 22, 0, tzinfo=PHT)
    # 12 slices: six discharging at 600 W, six charging at 300 W
    points = [_point(hour + timedelta(minutes=5 * i), battery_w=-600 if i < 6 else 300, power_w=1000)
              for i in range(12)]
    rows = _hourly_rows(SYSTEM, points)
    assert len(rows) == 1
    h = rows[0]
    assert h["points"] == 12
    assert h["battery_discharge_kwh"] == 0.3            # 6 × 600 W × 5 min
    assert h["battery_charge_kwh"] == 0.15              # 6 × 300 W × 5 min
    assert h["production_kwh"] == 1.0


def test_field_lists_agree():
    # The worker's hour, the API's hour and the DB roll-up must sum the same energies.
    assert set(BATTERY_ENERGY_KEYS) <= set(HOUR_SUM_FIELDS)
    assert tuple(HOUR_SUM_FIELDS) == tuple(HOURLY_FIELDS)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
