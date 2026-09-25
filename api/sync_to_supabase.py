"""
Solis → Supabase Daily Sync

Runs once daily (10:00 AM PHT via Render cron) for all users that have a
solis_station_id mapped in user_profiles.  Uses the Solis stationMonth API
to get pre-computed daily summaries (production, consumption, grid, earning)
and upserts them into Supabase energy_readings.

Real-time data (Today chart, current power) comes from /app/live which
hits Solis on-demand — no need for frequent syncing.

Usage:
    # One-shot sync (used by cron)
    python -m api.sync_to_supabase

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
from typing import List, Optional

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


async def sync_once(solis: SolisCloudClient, sb: Client) -> int:
    """Run one sync cycle. Returns number of readings written."""

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
            .select("id, user_id, solis_station_id, system_name, capacity_kwp, installation_date")
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

    now = datetime.now(PHT)
    current_month = now.strftime("%Y-%m")

    for station_row in stations:
        system_id: str = station_row["id"]
        user_id: str = station_row["user_id"]
        station_id: str = str(station_row["solis_station_id"]).strip()
        name: str = station_row.get("system_name") or station_id

        try:
            # 2a. Fetch station detail for system metadata
            station = await solis.station_detail(station_id)
            capacity_kwp = float(station.get("capacity") or 0)
            station_name = station.get("stationName") or station.get("sno") or station_id

            # 2a-bis. Best-effort battery capacity from inverterDetail (kWh)
            battery_capacity_kwh = await _fetch_battery_capacity_kwh(solis, station_id)

            # 2b. Fetch current month's daily summaries (1 API call, all days)
            month_data = await solis.station_month(station_id, current_month)

            if not month_data or not isinstance(month_data, list):
                log.warning("No stationMonth data for %s — skipping.", name)
                continue

            # 3. Refresh this station's metadata from Solis.
            #
            # This block used to be "find an active solar_systems row for this
            # user, else INSERT one". It no longer creates anything, for two
            # reasons. First, we are iterating solar_systems, so the row is
            # guaranteed to exist. Second, that INSERT was half of a live race:
            # this cron fires at 18:00 UTC and the 15-minute cron at 18:00 and
            # 18:15, both ran "find, else INSERT" with no unique constraint, and
            # both won. It produced 104 duplicate rows on 2026-08-20 and
            # 2026-08-10 — every pair identifiable by its address placeholder,
            # '-' from the 15-minute sync and '—' from this line. Creating
            # stations is now the job of onboarding alone, and migration 04's
            # UNIQUE(user_id, solis_station_id) makes a double-insert impossible
            # rather than merely unlikely.
            update_payload = {
                "system_name": station_name,
                "capacity_kwp": capacity_kwp,
                "solis_plant_name": station_name,
            }
            if battery_capacity_kwh is not None:
                update_payload["battery_capacity_kwh"] = battery_capacity_kwh

            # Correct a placeholder installation_date using this STATION's own
            # oldest reading. Keyed on system_id: for a multi-station customer,
            # the user's oldest reading may belong to a different station and
            # would backdate this one to before it was installed. The current
            # value rides on the stations query above (one round trip per
            # station saved, ~600 a night).
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

            sb.table("solar_systems").update(update_payload).eq("id", system_id).execute()

            # 4. One row per stationMonth day, upserted on (system_id,
            #    "timestamp") in batches across stations. This used to be a
            #    SELECT ("does this station have a row on this date?") followed
            #    by an UPDATE or INSERT, per day, per station. The station key
            #    matters: keyed on user_id, a customer's second station
            #    matched the first station's row and overwrote it. The unique
            #    index energy_readings_system_ts_uk (migration 05) now carries
            #    that guarantee inside the upsert itself.
            rows = _month_rows(user_id, system_id, month_data, capacity_kwp)
            pending.extend(rows)
            log.info("Parsed %s | %d days | month=%s", name, len(rows), current_month)
            if len(pending) >= SUPABASE_BATCH_SIZE:
                written += _upsert_batches(sb, pending)
                pending = []

        except SolisCloudError as e:
            log.error("Solis API error for %s (station %s): %s", name, station_id, e)
        except Exception as e:
            log.error("Unexpected error for %s (station %s): %s", name, station_id, e)

    if pending:
        written += _upsert_batches(sb, pending)

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


async def main() -> None:
    sb = build_supabase()
    solis = build_solis()

    if "--loop" in sys.argv:
        await run_loop(solis, sb)
    else:
        # Pre-step: onboard any Odoo lead that has a Solis station id so its
        # readings are included in this same sync run.
        if "--no-onboard" not in sys.argv:
            try:
                from api.onboard_from_odoo import auto_onboard_from_odoo
                log.info("Starting Odoo → Supabase auto-onboarding…")
                await auto_onboard_from_odoo(sb, solis)
            except Exception as e:
                log.error("Odoo auto-onboarding failed: %s", e)

        count = await sync_once(solis, sb)
        log.info("Done — %d reading(s) written.", count)

        # Sync referral codes from Odoo → user_profiles
        try:
            from api.sync_referral_codes import sync_referral_codes
            log.info("Starting referral code sync from Odoo…")
            sync_referral_codes(sb)
        except Exception as e:
            log.error("Referral code sync failed: %s", e)

        # Mirror Odoo lead/partner fields and Solis plant name/email into our
        # cached identity columns for Monitoring Admin. Read-only toward Odoo.
        # Last, so a failure here never delays the readings above.
        if "--no-mirror" not in sys.argv:
            try:
                from api.sync_identity_mirror import run as mirror_identity
                log.info("Starting identity mirror (Odoo + Solis → cached columns)…")
                await mirror_identity(apply=True)
            except Exception as e:
                log.error("Identity mirror failed: %s", e)


if __name__ == "__main__":
    asyncio.run(main())
