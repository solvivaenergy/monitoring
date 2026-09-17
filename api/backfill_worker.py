"""
Drain the backfill_jobs queue.

The back office never runs a backfill inline — an engineer's button writes a
row into backfill_jobs (migration 09) and this worker picks it up. Firing
backfills inline is how the August duplicate-station incident happened, and a
60-day fetch against an API that 502s does not fit inside an HTTP request.

    python -m api.backfill_worker              # drain until empty or budget spent
    python -m api.backfill_worker --once       # run at most one job
    python -m api.backfill_worker --budget 600 # seconds (default 720, for a 15-min cron)

Per job:
  * claim it with FOR UPDATE SKIP LOCKED (two workers never take the same job),
  * fetch from Solis for the requested range,
  * upsert on (system_id, "timestamp") — an engineer-requested backfill is a
    REFRESH: after a station id was corrected, the rows already there carry the
    wrong plant's numbers under this system, so existing rows are overwritten
    (unlike api/backfill_gaps.py, which fills holes and never touches a row),
  * mark succeeded/failed with rows_written and the error text; the 09 trigger
    mirrors the outcome onto solar_systems.last_backfill_*.

Granularities:
  daily         stationMonth per month in range → one energy_readings row per
                day, noon Manila (04:00Z), same row shape as the nightly sync.
  five_minutes  stationDay per day → energy_readings_five_minutes. That table is
                a rolling one-Manila-day cache purged every midnight, so only
                the current Manila day is meaningful; earlier days in the range
                are skipped and reported in the job's error field.

Job status changes are written through db.audited() with source
'backfill_worker', so the audit trail shows the worker acting on the
engineer's request_id.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

from supabase import create_client  # noqa: E402

from api import db  # noqa: E402
from api.backfill_history import PHT, SUPABASE_BATCH_SIZE, parse_month_day  # noqa: E402
from api.solis_client import SolisCloudClient, SolisCloudError  # noqa: E402
from api.sync_five_minutes_to_supabase import _build_row, _extract_lifetime_earning  # noqa: E402

log = logging.getLogger("backfill_worker")

SOLIS_CONCURRENCY = 2
SOLIS_DELAY = 0.5
MAX_ATTEMPTS = 3


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing env var: {key}")
    return val


def _months(d0: date, d1: date) -> List[str]:
    out, d = [], d0.replace(day=1)
    while d <= d1:
        out.append(d.strftime("%Y-%m"))
        d = (d.replace(year=d.year + 1, month=1) if d.month == 12 else d.replace(month=d.month + 1))
    return out


def _upsert(sb, table: str, rows: List[dict]) -> int:
    written = 0
    for i in range(0, len(rows), SUPABASE_BATCH_SIZE):
        batch = rows[i:i + SUPABASE_BATCH_SIZE]
        for attempt in range(5):
            try:
                sb.table(table).upsert(batch, on_conflict="system_id,timestamp").execute()
                written += len(batch)
                break
            except Exception as exc:
                if attempt == 4:
                    raise
                wait = 2 ** (attempt + 1)
                log.warning("upsert into %s failed (attempt %d/5), retrying in %ds: %s", table, attempt + 1, wait, str(exc)[:120])
                time.sleep(wait)
    return written


async def _fetch(sem: asyncio.Semaphore, coro_factory):
    async with sem:
        await asyncio.sleep(SOLIS_DELAY)
        return await coro_factory()


async def run_daily(job: Dict[str, Any], sb, solis: SolisCloudClient, sem: asyncio.Semaphore,
                    system: Dict[str, Any]) -> tuple[int, List[str]]:
    d0, d1 = job["date_from"], job["date_to"]
    months = _months(d0, d1)
    results = await asyncio.gather(*(_fetch(sem, lambda m=m: solis.station_month(job["solis_station_id"], m)) for m in months),
                                   return_exceptions=True)
    rows, notes = [], []
    capacity = float(system.get("capacity_kwp") or 0)
    by_day: Dict[str, dict] = {}
    for m, data in zip(months, results):
        if isinstance(data, Exception):
            notes.append(f"{m}: {str(data)[:80]}")
            continue
        if not isinstance(data, list):
            notes.append(f"{m}: no data")
            continue
        for day in data:
            parsed = parse_month_day(day, capacity)
            if parsed is None:
                continue
            ds = parsed.pop("date_str")
            if ds < d0.isoformat() or ds > d1.isoformat():
                continue
            y, mo, dd = (int(x) for x in ds.split("-"))
            by_day[ds] = {"user_id": str(system["user_id"]), "system_id": str(system["id"]),
                          "timestamp": datetime(y, mo, dd, 12, 0, 0, tzinfo=PHT).isoformat(), **parsed}
    rows = list(by_day.values())
    written = _upsert(sb, "energy_readings", rows) if rows else 0
    return written, notes


async def run_five_minutes(job: Dict[str, Any], sb, solis: SolisCloudClient, sem: asyncio.Semaphore,
                           system: Dict[str, Any]) -> tuple[int, List[str]]:
    today = datetime.now(PHT).date()
    d0, d1 = job["date_from"], job["date_to"]
    notes = []
    if d1 < today or d0 > today:
        return 0, [f"five-minute table holds only the current Manila day ({today}); range {d0}..{d1} skipped"]
    if d0 < today:
        notes.append(f"days before {today} skipped — the five-minute table is a one-day rolling cache")
    sid = job["solis_station_id"]
    day_data, station_all = await asyncio.gather(
        _fetch(sem, lambda: solis.station_day(sid, today.isoformat())),
        _fetch(sem, lambda: solis.station_all(sid)),
    )
    if not isinstance(day_data, list) or not day_data:
        return 0, notes + ["Solis returned no stationDay points for today"]
    lifetime = _extract_lifetime_earning(station_all) if station_all else None
    by_ts: Dict[int, dict] = {}
    for point in day_data:
        built = _build_row(str(system["user_id"]), str(system["id"]), point, lifetime_earning=lifetime)
        if built is not None:
            by_ts[built[0]] = built[1]
    rows = list(by_ts.values())
    return (_upsert(sb, "energy_readings_five_minutes", rows) if rows else 0), notes


def claim_next(conn) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """update public.backfill_jobs
              set status = 'running', started_at = now(), attempt = attempt + 1, error = null
            where id = (select id from public.backfill_jobs
                         where status = 'queued' order by queued_at
                         for update skip locked limit 1)
        returning *""").fetchone()
    return dict(row) if row else None


async def process(job: Dict[str, Any], sb, solis, sem) -> None:
    started = time.time()
    with db.connect(autocommit=True) as conn:
        system = conn.execute(
            "select id, user_id, capacity_kwp, solis_station_id from public.solar_systems where id = %s",
            (job["system_id"],)).fetchone()
    if not system:
        return _finish(job, "failed", 0, "system no longer exists")
    if not job.get("solis_station_id"):
        job["solis_station_id"] = system["solis_station_id"]
    if not job["solis_station_id"]:
        return _finish(job, "failed", 0, "system has no Solis station id")

    try:
        if job["granularity"] == "five_minutes":
            written, notes = await run_five_minutes(job, sb, solis, sem, system)
        else:
            written, notes = await run_daily(job, sb, solis, sem, system)
        status = "succeeded" if (written > 0 or not notes) else "failed"
        _finish(job, status, written, "; ".join(notes)[:900] or None)
        log.info("job %s %s: %d row(s) in %.0fs %s", job["id"], status, written, time.time() - started, notes[:3])
    except SolisCloudError as exc:
        _retry_or_fail(job, f"Solis: {exc}")
    except Exception as exc:
        log.exception("job %s crashed", job["id"])
        _retry_or_fail(job, f"{type(exc).__name__}: {str(exc)[:300]}")


def _finish(job: Dict[str, Any], status: str, written: int, error: Optional[str]) -> None:
    with db.audited(str(job.get("requested_by") or "") or None, None,
                    f"backfill {status}: {job.get('requested_reason') or ''}"[:500],
                    source="backfill_worker", request_id=str(job.get("request_id") or "") or None) as conn:
        conn.execute(
            "update public.backfill_jobs set status = %s, rows_written = %s, error = %s, finished_at = now() where id = %s",
            (status, written, error, job["id"]))


def _retry_or_fail(job: Dict[str, Any], error: str) -> None:
    if job.get("attempt", 1) < MAX_ATTEMPTS:
        with db.audited(None, None, "backfill retry scheduled", source="backfill_worker") as conn:
            conn.execute("update public.backfill_jobs set status = 'queued', error = %s where id = %s",
                         (f"attempt {job.get('attempt')}: {error}"[:900], job["id"]))
        log.warning("job %s re-queued after: %s", job["id"], error[:160])
    else:
        _finish(job, "failed", 0, error[:900])
        log.error("job %s failed permanently: %s", job["id"], error[:160])


async def main() -> None:
    ap = argparse.ArgumentParser(description="Drain backfill_jobs")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--budget", type=int, default=720, help="seconds to keep claiming jobs")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    sb = create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))
    solis = SolisCloudClient(key_id=get_env("SOLIS_CLOUD_KEY_ID"), key_secret=get_env("SOLIS_CLOUD_KEY_SECRET"))
    sem = asyncio.Semaphore(SOLIS_CONCURRENCY)
    deadline = time.time() + args.budget
    done = 0
    while time.time() < deadline:
        # Audited so the queued→running transition reads actor_kind='job',
        # source='backfill_worker' rather than 'unknown'.
        with db.audited(None, None, "claimed by backfill worker", source="backfill_worker") as conn:
            job = claim_next(conn)
        if not job:
            break
        await process(job, sb, solis, sem)
        done += 1
        if args.once:
            break
    log.info("worker finished: %d job(s) processed", done)


if __name__ == "__main__":
    asyncio.run(main())
