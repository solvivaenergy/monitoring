"""
Re-queue the hourly history for stations whose fleet load hit Solis rate limits.

The 2026-10-03 90-day hourly load (request d0177020…) ran next to the
five-minute cron with no shared rate gate, and Solis answered 429 on 128
station-days across 31 stations. The worker records each such day in the
job's error text as "YYYY-MM-DD: 429". This script reads those notes and
queues one new hourly job per affected station covering the span from its
first to its last rate-limited day — far cheaper than re-reading 90 days.

    python -m api.requeue_rate_limited_hourly <request-id> [--dry-run]

Stations that already have an hourly job queued or running are skipped.
"""

from __future__ import annotations

import argparse
import logging
import re
import uuid
from datetime import date

from dotenv import load_dotenv

load_dotenv()

from api import db  # noqa: E402

log = logging.getLogger("requeue_rate_limited_hourly")
DAY_429 = re.compile(r"(\d{4}-\d{2}-\d{2}): 429")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Re-queue hourly jobs for the station-days Solis rate-limited.")
    ap.add_argument("request_id", help="request id of the fleet load whose jobs to inspect")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with db.connect(autocommit=True) as conn:
        jobs = conn.execute(
            """select j.system_id, j.solis_station_id, j.error,
                      exists (select 1 from public.backfill_jobs b
                               where b.system_id = j.system_id and b.granularity = 'hourly'
                                 and b.status in ('queued', 'running')) as busy
                 from public.backfill_jobs j
                where j.request_id = %s and j.granularity = 'hourly' and j.error like '%%429%%'""",
            (args.request_id,)).fetchall()

    plan = []
    for j in jobs:
        days = sorted(date.fromisoformat(d) for d in DAY_429.findall(j["error"] or ""))
        if not days:
            continue
        plan.append((j, days[0], days[-1], len(days)))
    log.info("%d station(s) with rate-limited days; %d busy (skipped); %d station-days to re-read.",
             len(plan), sum(1 for p in plan if p[0]["busy"]), sum(p[3] for p in plan))
    for j, d0, d1, n in plan:
        log.info("  %s  %s → %s  (%d day(s) hit 429)%s", j["solis_station_id"], d0, d1, n, "  BUSY, skipped" if j["busy"] else "")
    targets = [(j, d0, d1) for j, d0, d1, _ in plan if not j["busy"]]
    if args.dry_run or not targets:
        return

    request_id = str(uuid.uuid4())
    reason = f"hourly re-read of the days Solis rate-limited (429) during fleet load {args.request_id[:8]}"
    with db.audited(None, None, reason, source="fleet_script", request_id=request_id) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """insert into public.backfill_jobs
                     (system_id, solis_station_id, granularity, date_from, date_to,
                      requested_by, requested_reason, request_id)
                   values (%s, %s, 'hourly', %s, %s, null, %s, %s)
                   on conflict do nothing""",
                [(j["system_id"], j["solis_station_id"], d0, d1, reason, request_id) for j, d0, d1 in targets])
    log.info("Queued %d hourly job(s) under request %s.", len(targets), request_id)


if __name__ == "__main__":
    main()
