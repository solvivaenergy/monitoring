"""
Solis → Supabase Daily Sync

Runs nightly at 02:00 Manila (api/worker.py) for every active station in
solar_systems. Since 2026-10-10 the daily summaries (production, consumption,
grid, battery, earning) come from Solis's stationDayEnergyList — one record
per plant per date, 100 plants a call — for today and the NIGHTLY_REREAD_DAYS
(7) days before it, and are upserted into energy_readings. Station metadata
(capacity, plant name, battery kWh) is refreshed weekly. The previous path —
one stationMonth call per station, the whole month re-upserted every night,
~2,100 Solis calls and 40–50 minutes — stays as the automatic fallback when
the bulk endpoint fails (or with NIGHTLY_DAILY_SOURCE=month).

Real-time data (Today chart, current power) comes from the five-minute sync
and /app/live — no need for frequent syncing here.

Usage:
    # The nightly chain (onboarding → daily readings → referral codes → mirror)
    python -m api.sync_to_supabase

    # The daily-readings step alone, writing nothing (counts in the log)
    python -m api.sync_to_supabase --dry-run

    # Continuous (legacy, not recommended)
    python -m api.sync_to_supabase --loop

Environment variables:
    SOLIS_CLOUD_KEY_ID       – Solis Cloud API key ID
    SOLIS_CLOUD_KEY_SECRET   – Solis Cloud API secret
    SUPABASE_URL             – Supabase project URL
    SUPABASE_SERVICE_KEY     – Supabase service-role key (bypasses RLS)
"""

import asyncio
import os
import sys
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

from api.solis_client import SolisCloudClient, SolisCloudError
# The one stationMonth-day → energy_readings row parser, shared with the
# backfill worker and the history backfill so all three write the same shape.
from api.backfill_history import parse_month_day

# supabase-py (sync client)
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("solis_sync")

SYNC_INTERVAL_SECONDS = 900  # 15 minutes
SUPABASE_BATCH_SIZE = 500
PHT = timezone(timedelta(hours=8))

# Days before today re-read every night (fix 3, 2026-10-10). Solis's figure for
# a day is provisional at 02:00 Manila — often still 0, or an inverter value it
# later settles to a whole kWh — so recent days must be re-read; 7 always spans
# a month boundary (the 2026-09-30 incident: 603 of 621 month-end rows stayed
# 0 because nothing re-read the old month). Days older than this are settled
# by the monthly task's full re-read of the previous month on the 1st.
NIGHTLY_REREAD_DAYS = int(os.getenv("NIGHTLY_REREAD_DAYS", "7"))
# "bulk" = stationDayEnergyList per date (default); "month" = the old
# stationMonth-per-station path. Bulk falls back to month by itself on error.
NIGHTLY_DAILY_SOURCE = os.getenv("NIGHTLY_DAILY_SOURCE", "bulk").strip().lower()
# Station metadata (stationDetail + inverterDetail) for every station on this
# weekday (Monday = 0 … Sunday = 6); other nights only new stations and
# stations without a capacity get it.
STATION_METADATA_REFRESH_WEEKDAY = int(os.getenv("STATION_METADATA_REFRESH_WEEKDAY", "6"))
# A station's first nights: its capacity arrives with the first stationDetail
# and the installation-date correction needs its own readings to exist. Seven
# days covers that; 96 of 700 stations were under 30 days old on 2026-10-10,
# which would have kept ~290 sequential Solis calls in every nightly.
STATION_METADATA_NEW_DAYS = 7
BULK_PAGE_SIZE = 100


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


def build_supabase() -> Client:
    return create_client(
        get_env("SUPABASE_URL"),
        get_env("SUPABASE_SERVICE_KEY"),
    )


def build_solis() -> SolisCloudClient:
    return SolisCloudClient(
        key_id=get_env("SOLIS_CLOUD_KEY_ID"),
        key_secret=get_env("SOLIS_CLOUD_KEY_SECRET"),
    )


def _month_rows(user_id: str, system_id: str, month_data: list, capacity_kwp: float) -> List[dict]:
    """One energy_readings row per stationMonth day, noon Manila (04:00Z).

    Same shape and the same consumption/full-load-hours rules as before (they
    live in parse_month_day, which the backfill worker also uses). Keyed on
    the date so a day Solis repeats under two adjacent months keeps its last
    copy — an upsert batch with a duplicate key is rejected whole.
    """
    by_day: dict = {}
    for day in month_data:
        parsed = parse_month_day(day, capacity_kwp)
        if parsed is None:
            continue
        ds = parsed.pop("date_str")
        y, m, d = (int(x) for x in ds.split("-"))
        by_day[ds] = {
            "user_id": user_id,
            "system_id": system_id,
            "timestamp": datetime(y, m, d, 12, 0, 0, tzinfo=PHT).isoformat(),
            **parsed,
        }
    return list(by_day.values())


def _upsert_batches(sb: Client, rows: List[dict]) -> int:
    """Upsert on (system_id, "timestamp") in 500-row batches, 3 attempts each.
    Returns the number of rows in batches that succeeded.

    This replaces one SELECT plus one UPDATE (or INSERT) per station-day —
    about 13,000 round trips and 13,000 single-row commits a night, each with
    its own WAL flush (measured 2026-09-24: 95,850 selects and 158,000
    single-row updates in 22 days). ~30 requests do the same work. Safe because
    every current-month row sits at noon: 14,806 rows, 0 off-noon, checked the
    same day. The 405 legacy wall-clock rows are all older than any month this
    sync touches (see migration 12).
    """
    done = 0
    for i in range(0, len(rows), SUPABASE_BATCH_SIZE):
        batch = rows[i:i + SUPABASE_BATCH_SIZE]
        for attempt in range(3):
            try:
                sb.table("energy_readings").upsert(
                    batch, on_conflict="system_id,timestamp", returning="minimal"
                ).execute()
                done += len(batch)
                break
            except Exception as exc:
                if attempt == 2:
                    log.error("energy_readings upsert of %d row(s) failed after 3 attempts: %s",
                              len(batch), str(exc)[:200])
                else:
                    wait = 2 ** (attempt + 1)
                    log.warning("energy_readings upsert failed (attempt %d/3), retrying in %ds: %s",
                                attempt + 1, wait, str(exc)[:120])
                    time.sleep(wait)
    return done


async def _fetch_battery_capacity_kwh(
    solis: SolisCloudClient, station_id: str
) -> Optional[float]:
    """Best-effort lookup of the battery pack's rated capacity in kWh.

    Tries the first inverter on the station and inspects several Solis fields
    that may carry capacity info. Returns None if the station has no battery
    or the value can't be determined.
    """
    try:
        inv_list = await solis.list_inverters(station_id, page_size=1)
    except Exception as e:
        log.debug("list_inverters failed for station %s: %s", station_id, e)
        return None

    records = None
    if isinstance(inv_list, dict):
        page = inv_list.get("page")
        if isinstance(page, dict):
            records = page.get("records")
        if not records:
            records = inv_list.get("records")
    if not records:
        return None

    inverter_id = records[0].get("id") or records[0].get("sn")
    if not inverter_id:
        return None

    try:
        detail = await solis.inverter_detail(str(inverter_id))
    except Exception as e:
        log.debug("inverter_detail failed for %s: %s", inverter_id, e)
        return None

    if not isinstance(detail, dict):
        return None

    # 1) Direct kWh fields some Solis firmwares expose
    for key in ("batteryCapacityKwh", "batteryCapacityEnergy", "batteryTotalCapacity"):
        val = detail.get(key)
        if val not in (None, "", 0, "0"):
            try:
                return round(float(val), 2)
            except (TypeError, ValueError):
                pass

    # 2) Derive from Ah * V (most common)
    ah = detail.get("storageBatteryCapacity") or detail.get("batteryCapacity")
    v = detail.get("storageBatteryVoltage") or detail.get("batteryVoltage")
    try:
        if ah and v:
            kwh = float(ah) * float(v) / 1000.0
            if kwh > 0:
                return round(kwh, 2)
    except (TypeError, ValueError):
        pass

    return None


def _reread_dates(now: datetime, days: int) -> List[str]:
    """Today and the `days` days before it, newest first, as YYYY-MM-DD.

    Today is the 0 kWh placeholder row the portal patches from the live curve
    (HomePage's patchToday also copes with no row at all); the days before it
    are the provisional ones. See NIGHTLY_REREAD_DAYS.
    """
    today = now.date()
    return [(today - timedelta(days=i)).isoformat() for i in range(max(0, days) + 1)]


async def _fetch_bulk_days(solis: SolisCloudClient, dates: List[str]) -> Dict[str, Dict[str, dict]]:
    """{station_id: {date_str: day record}} for every plant on the account,
    from stationDayEnergyList — `pages` calls of 100 per date (8 for the
    710-plant fleet) instead of one stationMonth call per station. The records
    carry the same per-day fields as stationMonth and list the same plant-days
    (verified 2026-10-10: the 662 fleet plants with a 2026-10-09 row in our
    database were exactly the 662 the endpoint listed). A plant with no data on
    a date is absent from that date, as stationMonth omits such a day."""
    out: Dict[str, Dict[str, dict]] = {}
    for ds in dates:
        page, pages, n = 1, 1, 0
        while True:
            resp = await solis.station_day_energy_list(ds, page_no=page, page_size=BULK_PAGE_SIZE)
            resp = resp if isinstance(resp, dict) else {}
            for rec in resp.get("records") or []:
                sid = str(rec.get("id") or "").strip()
                if sid:
                    out.setdefault(sid, {})[ds] = rec
                    n += 1
            # Page by `pages`, never by the record count: a page holds fewer
            # than pageSize records when plants had no data that day, and the
            # response's `current` always reads 1.
            pages = int(resp.get("pages") or 1)
            if page >= pages:
                break
            page += 1
        log.info("stationDayEnergyList %s: %d plant(s) in %d page(s)", ds, n, pages)
    return out


def _wants_metadata(station_row: dict, now: datetime) -> bool:
    """stationDetail + inverterDetail (capacity, plant name, battery kWh, the
    installation-date correction) once a week for everyone, nightly only for
    stations that still lack a capacity or were created in the last 7 days.
    Until 2026-10-10 this ran for every station every night: 1,400 of the
    nightly's ~2,100 Solis calls and ~700 solar_systems UPDATEs for values
    that change a few times a year."""
    if now.weekday() == STATION_METADATA_REFRESH_WEEKDAY:
        return True
    if not float(station_row.get("capacity_kwp") or 0):
        return True
    created = str(station_row.get("created_at") or "")[:10]
    return bool(created) and created >= (now - timedelta(days=STATION_METADATA_NEW_DAYS)).strftime("%Y-%m-%d")


async def sync_once(solis: SolisCloudClient, sb: Client, dry_run: bool = False) -> int:
    """Run one sync cycle. Returns number of readings written (or, with
    dry_run, the number that would have been — nothing is written then)."""

    # 1. Get every active station. The unit of work is a STATION, not a user:
    #    a customer can own several (Arnel Cipriano Chavez has 3). Sourcing
    #    from user_profiles.solis_station_id — a single scalar column — made a
    #    second station structurally unreachable, and picking the system with
    #    an unordered .limit(1) per user wrote every station's data onto
    #    whichever row Postgres happened to return first.
    #
    #    Paginated deliberately: PostgREST caps an unbounded select at 1000
    #    rows and truncates SILENTLY. At 719 systems today that is invisible;
    #    it would start dropping stations from the nightly sync with no error
    #    the moment the fleet passes 1000.
    stations = []
    page_size = 1000
    offset = 0
    while True:
        page = (
            sb.table("solar_systems")
            .select("id, user_id, solis_station_id, system_name, capacity_kwp, installation_date, created_at")
            .not_.is_("solis_station_id", "null")
            .eq("status", "active")
            .order("id")
            .range(offset, offset + page_size - 1)
            .execute()
        ).data or []
        stations.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    if not stations:
        log.info("No active stations with a solis_station_id — nothing to sync.")
        return 0

    owners = {s["user_id"] for s in stations}
    log.info("Found %d station(s) across %d customer(s) to sync.", len(stations), len(owners))
    written = 0
    pending: List[dict] = []
    metadata_refreshed = 0
    no_days = 0

    now = datetime.now(PHT)
    # The re-read window (fix 3, 2026-10-10): today and the NIGHTLY_REREAD_DAYS
    # before it, for every plant on the account, in `pages` bulk calls per
    # date. Until now every night re-upserted the WHOLE current month from one
    # stationMonth call per station (~20k rows by month end, ~700 calls), plus
    # the previous month on days 1–3.
    dates = _reread_dates(now, NIGHTLY_REREAD_DAYS)
    bulk: Optional[Dict[str, Dict[str, dict]]] = None
    if NIGHTLY_DAILY_SOURCE != "month":
        try:
            bulk = await _fetch_bulk_days(solis, dates)
            log.info("Bulk day records: %d plant(s) over %d date(s) (%s..%s).",
                     len(bulk), len(dates), dates[-1], dates[0])
        except Exception as e:
            log.error("stationDayEnergyList failed (%s) — falling back to stationMonth per station for this run.",
                      str(e)[:160])
    months: List[str] = []
    if bulk is None:
        # The pre-2026-10-10 path, kept as the fallback: the whole current
        # month, plus the previous month on its first days (Solis's figure for
        # "yesterday" at 02:00 Manila is provisional; across a month boundary
        # nothing re-read the old month, so 2026-09-30 kept 603 of 621 rows at
        # 0 — found 2026-10-02).
        months = [now.strftime("%Y-%m")]
        if now.day <= int(os.getenv("PREVIOUS_MONTH_REFRESH_DAYS", "3")):
            months.insert(0, (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m"))

    for station_row in stations:
        system_id: str = station_row["id"]
        user_id: str = station_row["user_id"]
        station_id: str = str(station_row["solis_station_id"]).strip()
        name: str = station_row.get("system_name") or station_id

        try:
            # 2a. Station metadata — capacity, plant name, battery kWh and the
            #     installation-date correction — weekly for everyone, nightly
            #     only for new stations and stations without a capacity (see
            #     _wants_metadata). The capacity on the row is what the daily
            #     rows' full-load-hours fall back to on the other nights.
            capacity_kwp = float(station_row.get("capacity_kwp") or 0)
            if _wants_metadata(station_row, now):
                station = await solis.station_detail(station_id)
                capacity_kwp = float(station.get("capacity") or 0)
                station_name = station.get("stationName") or station.get("sno") or station_id
                battery_capacity_kwh = await _fetch_battery_capacity_kwh(solis, station_id)

                # This block used to be "find an active solar_systems row for
                # this user, else INSERT one". It no longer creates anything,
                # for two reasons. First, we are iterating solar_systems, so
                # the row is guaranteed to exist. Second, that INSERT was half
                # of a live race: this cron fired at 18:00 UTC and the
                # 15-minute cron at 18:00 and 18:15, both ran "find, else
                # INSERT" with no unique constraint, and both won. It produced
                # 104 duplicate rows on 2026-08-20 and 2026-08-10 — every pair
                # identifiable by its address placeholder, '-' from the
                # 15-minute sync and '—' from this line. Creating stations is
                # now the job of onboarding alone, and migration 04's
                # UNIQUE(user_id, solis_station_id) makes a double-insert
                # impossible rather than merely unlikely.
                update_payload = {
                    "system_name": station_name,
                    "capacity_kwp": capacity_kwp,
                    "solis_plant_name": station_name,
                }
                if battery_capacity_kwh is not None:
                    update_payload["battery_capacity_kwh"] = battery_capacity_kwh

                # Correct a placeholder installation_date using this STATION's
                # own oldest reading. Keyed on system_id: for a multi-station
                # customer, the user's oldest reading may belong to a different
                # station and would backdate this one to before it was
                # installed. The current value rides on the stations query
                # above (one round trip per station saved).
                current_install = station_row.get("installation_date")
                if current_install and current_install >= (now - timedelta(days=30)).strftime("%Y-%m-%d"):
                    oldest = (
                        sb.table("energy_readings")
                        .select("timestamp")
                        .eq("system_id", system_id)
                        .order("timestamp", desc=False)
                        .limit(1)
                        .execute()
                    )
                    update_payload["installation_date"] = (
                        oldest.data[0]["timestamp"][:10] if oldest.data
                        else now.strftime("%Y-%m-%d")
                    )

                if not dry_run:
                    sb.table("solar_systems").update(update_payload).eq("id", system_id).execute()
                metadata_refreshed += 1

            # 2b. The daily summaries. Bulk: this station's records for the
            #     re-read window, fetched for the whole account above — no
            #     Solis call here. Fallback: one stationMonth call per month,
            #     every day of the month (the pre-2026-10-10 path).
            if bulk is not None:
                day_records = list(bulk.get(station_id, {}).values())
                source = f"{dates[-1]}..{dates[0]} via stationDayEnergyList"
            else:
                day_records = []
                for mon in months:
                    data = await solis.station_month(station_id, mon)
                    if data and isinstance(data, list):
                        day_records.extend(data)
                source = "month=" + "+".join(months)

            if not day_records:
                no_days += 1
                log.info("No Solis day record for %s in the window — skipping.", name)
                continue

            # 3. One row per Solis day, upserted on (system_id, "timestamp") in
            #    batches across stations. This used to be a SELECT ("does this
            #    station have a row on this date?") followed by an UPDATE or
            #    INSERT, per day, per station. The station key matters: keyed
            #    on user_id, a customer's second station matched the first
            #    station's row and overwrote it. The unique index
            #    energy_readings_system_ts_uk (migration 05) now carries that
            #    guarantee inside the upsert itself.
            rows = _month_rows(user_id, system_id, day_records, capacity_kwp)
            pending.extend(rows)
            log.info("Parsed %s | %d day(s) | %s", name, len(rows), source)
            if len(pending) >= SUPABASE_BATCH_SIZE:
                written += len(pending) if dry_run else _upsert_batches(sb, pending)
                pending = []

        except SolisCloudError as e:
            log.error("Solis API error for %s (station %s): %s", name, station_id, e)
        except Exception as e:
            log.error("Unexpected error for %s (station %s): %s", name, station_id, e)

    if pending:
        written += len(pending) if dry_run else _upsert_batches(sb, pending)

    log.info(
        "Daily readings: %d row(s) %s for %d station(s), %s; metadata refreshed for %d; "
        "%d station(s) had no Solis day in the window.%s",
        written, "would be written" if dry_run else "written", len(stations),
        (f"{len(dates)} date(s) via stationDayEnergyList" if bulk is not None
         else "month(s) " + "+".join(months) + " via stationMonth"),
        metadata_refreshed, no_days, " [dry-run]" if dry_run else "",
    )
    return written


async def run_loop(solis: SolisCloudClient, sb: Client) -> None:
    """Continuously sync every 5 minutes."""
    log.info("Starting continuous sync (interval=%ds)…", SYNC_INTERVAL_SECONDS)
    while True:
        try:
            count = await sync_once(solis, sb)
            log.info("Cycle complete — %d reading(s) written. Sleeping %ds…", count, SYNC_INTERVAL_SECONDS)
        except Exception as e:
            log.error("Sync cycle failed: %s — retrying next cycle.", e)
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


async def run_nightly(solis: SolisCloudClient, sb: Client, onboard: bool = True, mirror: bool = True) -> dict:
    """The nightly chain, in order: Odoo onboarding → daily readings → referral
    codes → identity mirror. Each step is isolated so a failure in one never
    stops the next; the returned dict says what happened to each. Called by the
    02:00 cron (main) and by api/worker.py."""
    stats: dict = {}

    # Pre-step: onboard any Odoo lead that has a Solis station id so its
    # readings are included in this same sync run.
    if onboard:
        try:
            from api.onboard_from_odoo import auto_onboard_from_odoo
            log.info("Starting Odoo → Supabase auto-onboarding…")
            await auto_onboard_from_odoo(sb, solis)
            stats["onboarding"] = "ok"
        except Exception as e:
            log.error("Odoo auto-onboarding failed: %s", e)
            stats["onboarding"] = f"failed: {str(e)[:120]}"

    count = await sync_once(solis, sb)
    log.info("Done — %d reading(s) written.", count)
    stats["daily_rows"] = count

    # Sync referral codes from Odoo → user_profiles
    try:
        from api.sync_referral_codes import sync_referral_codes
        log.info("Starting referral code sync from Odoo…")
        stats["referral_codes"] = sync_referral_codes(sb)
    except Exception as e:
        log.error("Referral code sync failed: %s", e)
        stats["referral_codes"] = f"failed: {str(e)[:120]}"

    # Mirror Odoo lead/partner fields and Solis plant name/email into our
    # cached identity columns for Monitoring Admin. Read-only toward Odoo.
    # Last, so a failure here never delays the readings above.
    if mirror:
        try:
            from api.sync_identity_mirror import run as mirror_identity
            log.info("Starting identity mirror (Odoo + Solis → cached columns)…")
            stats["identity_mirror"] = await mirror_identity(apply=True)
        except Exception as e:
            log.error("Identity mirror failed: %s", e)
            stats["identity_mirror"] = f"failed: {str(e)[:120]}"
    return stats


async def main() -> None:
    sb = build_supabase()
    solis = build_solis()
    try:
        if "--loop" in sys.argv:
            await run_loop(solis, sb)
        elif "--dry-run" in sys.argv:
            # The daily-readings step alone, writing nothing: the bulk reads
            # from Solis, row counts in the log. Onboarding and the identity
            # mirror are skipped.
            n = await sync_once(solis, sb, dry_run=True)
            log.info("Dry run: %d daily row(s) would be written.", n)
        else:
            await run_nightly(solis, sb, onboard="--no-onboard" not in sys.argv, mirror="--no-mirror" not in sys.argv)
    finally:
        await solis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
