"""The 2026-10-10 fixes, pinned without a network:

  2. lifetime-earning slices reach every station at the 15-minute fleet cadence;
  3. the nightly's re-read window and the bulk endpoint's paging/parsing;
  4. the five-minute pass chunks the fleet;
  8. the instance-metrics parser and the pressure thresholds.

Run with `python -m pytest tests` or plainly `python tests/test_fixes_2026_10_10.py`.
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.backfill_history import parse_month_day  # noqa: E402
from api.supabase_metrics import parse_instance_metrics, pressure_message  # noqa: E402
from api.sync_five_minutes_to_supabase import _chunked, _lifetime_slice_now  # noqa: E402
from api.sync_to_supabase import _fetch_bulk_days, _month_rows, _reread_dates, _wants_metadata  # noqa: E402

PHT = timezone(timedelta(hours=8))


# -- 2. lifetime slices ------------------------------------------------------

def test_fifteen_minute_fleet_passes_cover_every_slice_in_three_hours():
    seen = []
    for hour in range(3):
        for minute in (0, 15, 30, 45):
            seen.append(_lifetime_slice_now(datetime(2026, 10, 10, hour, minute, 7, tzinfo=PHT), 12, 15))
    assert sorted(seen) == list(range(12))
    # and the cycle repeats: hour 3 == hour 0
    assert _lifetime_slice_now(datetime(2026, 10, 10, 3, 15, tzinfo=PHT), 12, 15) == \
        _lifetime_slice_now(datetime(2026, 10, 10, 0, 15, tzinfo=PHT), 12, 15)


def test_old_schedule_only_reached_a_third_of_the_slices():
    # The bug: minute // 5 % 12 at :00/:15/:30/:45 → {0, 3, 6, 9} forever.
    old = {(m // 5) % 12 for m in (0, 15, 30, 45)}
    assert old == {0, 3, 6, 9}


def test_five_minute_fleet_cadence_is_the_original_schedule():
    for minute in range(0, 60, 5):
        now = datetime(2026, 10, 10, 14, minute, tzinfo=PHT)
        assert _lifetime_slice_now(now, 12, 5) == (minute // 5) % 12


# -- 3. nightly window + bulk endpoint ---------------------------------------

def test_reread_dates_are_today_and_the_days_before_newest_first():
    now = datetime(2026, 10, 3, 2, 0, tzinfo=PHT)
    dates = _reread_dates(now, 7)
    assert dates[0] == "2026-10-03" and dates[-1] == "2026-09-26" and len(dates) == 8
    assert "2026-09-30" in dates          # the month boundary is inside the window


def test_bulk_record_parses_exactly_like_a_station_month_day():
    rec = {"id": "1298491919450000001", "dateStr": "2026-10-09", "date": 1791475200000,
           "energy": 21.6, "consumeEnergy": 18.2, "homeLoadEnergy": 17.0, "homeGridEnergy": 0,
           "gridPurchasedEnergy": 3.3, "gridSellEnergy": 6.7, "batteryChargeEnergy": 4.1,
           "batteryDischargeEnergy": 3.9, "money": 151.2, "fullHour": 3.6}
    rows = _month_rows("u", "s", [rec], capacity_kwp=6.0)
    assert len(rows) == 1
    row = rows[0]
    assert row["timestamp"] == "2026-10-09T12:00:00+08:00"      # noon Manila = 04:00Z
    assert row["production_kwh"] == 21.6 and row["grid_import_kwh"] == 3.3 and row["grid_export_kwh"] == 6.7
    assert row["battery_charge_kwh"] == 4.1 and row["battery_discharge_kwh"] == 3.9
    assert row["daily_earning"] == 151.2 and row["full_load_hours"] == 3.6
    # same parser, same answer
    assert parse_month_day(rec, 6.0)["consumption_kwh"] == row["consumption_kwh"]


class _FakeSolis:
    """stationDayEnergyList as observed: pages hold FEWER records than
    pageSize, `current` always 1, `pages` is the only reliable cursor."""

    def __init__(self):
        self.calls = []

    async def station_day_energy_list(self, date_str, page_no=1, page_size=100):
        self.calls.append((date_str, page_no))
        pages = {1: [{"id": "a", "dateStr": date_str, "energy": 1}, {"id": "b", "dateStr": date_str, "energy": 2}],
                 2: [{"id": "c", "dateStr": date_str, "energy": 3}],
                 3: [{"id": 4, "dateStr": date_str, "energy": 4}]}
        return {"records": pages[page_no], "total": 4, "size": page_size, "current": 1, "pages": 3}


def test_bulk_paging_follows_pages_not_record_count():
    solis = _FakeSolis()
    out = asyncio.run(_fetch_bulk_days(solis, ["2026-10-09", "2026-10-08"]))
    assert solis.calls == [("2026-10-09", 1), ("2026-10-09", 2), ("2026-10-09", 3),
                           ("2026-10-08", 1), ("2026-10-08", 2), ("2026-10-08", 3)]
    assert set(out) == {"a", "b", "c", "4"}                 # ids normalised to str
    assert set(out["a"]) == {"2026-10-09", "2026-10-08"}


def test_metadata_is_weekly_or_for_new_and_capacity_less_stations():
    sunday = datetime(2026, 10, 11, 2, 0, tzinfo=PHT)
    monday = datetime(2026, 10, 12, 2, 0, tzinfo=PHT)
    old = {"capacity_kwp": 5.5, "created_at": "2026-03-01T00:00:00+00:00"}
    assert _wants_metadata(old, sunday) and not _wants_metadata(old, monday)
    assert _wants_metadata({"capacity_kwp": 0, "created_at": "2026-03-01"}, monday)
    assert _wants_metadata({"capacity_kwp": 5.5, "created_at": "2026-10-08T10:00:00+00:00"}, monday)      # 4 days old
    assert not _wants_metadata({"capacity_kwp": 5.5, "created_at": "2026-09-20T10:00:00+00:00"}, monday)  # 22 days old


# -- 4. chunked pass ---------------------------------------------------------

def test_chunking_keeps_order_and_covers_everything():
    plan = [(f"s{i}", i % 2 == 0) for i in range(703)]
    chunks = _chunked(plan, 200)
    assert [len(c) for c in chunks] == [200, 200, 200, 103]
    assert [u for c in chunks for u in c] == plan


# -- 8. instance metrics -----------------------------------------------------

SAMPLE = """# HELP node_memory_MemTotal_bytes Memory information field MemTotal_bytes.
node_memory_MemTotal_bytes{supabase_project_ref="x",service_type="db"} 1.924182016e+09
node_memory_MemAvailable_bytes{supabase_project_ref="x",service_type="db"} 8.34396160e+08
node_memory_SwapTotal_bytes{supabase_project_ref="x",service_type="db"} 1.073737728e+09
node_memory_SwapFree_bytes{supabase_project_ref="x",service_type="db"} 9.23041792e+08
node_load1{supabase_project_ref="x",service_type="db"} 0.01
node_filesystem_avail_bytes{device="/dev/nvme0n1p2",fstype="ext4",mountpoint="/"} 2.279190528e+09
node_filesystem_avail_bytes{device="/dev/nvme1n1",fstype="ext4",mountpoint="/data"} 6.992887808e+09
node_filesystem_size_bytes{device="/dev/nvme1n1",fstype="ext4",mountpoint="/data"} 8.350298112e+09
pg_database_size_mb{server="localhost:5432"} 582
pg_wal_size_mb{server="localhost:5432"} 544
"""


def test_metrics_parser_reads_the_dashboard_figures():
    m = parse_instance_metrics(SAMPLE)
    assert m["mem_total_mb"] == 1924 and m["mem_available_mb"] == 834
    assert m["swap_total_mb"] == 1074 and m["swap_used_mb"] == 151
    assert m["data_disk_free_gb"] == 7.0 and m["data_disk_total_gb"] == 8.4   # /data, not the root volume
    assert m["db_size_mb"] == 582 and m["wal_size_mb"] == 544 and m["load1"] == 0.01


def test_pressure_message_only_on_breach():
    m = parse_instance_metrics(SAMPLE)
    assert pressure_message(m, 300, 300) is None
    assert "free" in pressure_message(m, 900, 300)
    assert "swap" in pressure_message(m, 300, 100)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
