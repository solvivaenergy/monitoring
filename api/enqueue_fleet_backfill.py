"""
Monthly accuracy run: queue one daily backfill job per active station for the
PREVIOUS month. Render cron `solviva-monthly-refresh`, the 1st of each month at
09:00 Manila (01:00 UTC) — render.yaml. The ordinary backfill worker drains the
jobs over the following hour or so; nothing here talks to Solis.

Why: the nightly sync re-upserts only the current month, so the last day(s) of
a month kept their provisional values for good (2026-09-30: 603 of 621 rows
were still 0 on 2 Oct). The nightly now re-reads the previous month on days
1–3 as well; this run is the belt to those braces, and it is the guarantee
engineering asked for on 2026-10-02: every month, the whole month is re-read
from Solis once Solis has settled it.

    python -m api.enqueue_fleet_backfill              # previous month
    python -m api.enqueue_fleet_backfill 2026-09      # a given month
    python -m api.enqueue_fleet_backfill --dry-run    # count only

Same insert as POST /monitoring-admin/api/backfill/fleet, minus the staff
actor: requested_by is NULL and the audit source is 'monthly_refresh', so the
Backfills tab shows the batch as "monthly run".
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from api import db  # noqa: E402
from api.monitoring_admin_routes import FLEET_TARGETS_SQL  # noqa: E402

log = logging.getLogger("enqueue_fleet_backfill")
PHT = timezone(timedelta(hours=8))


def month_bounds(ym: str | None) -> tuple[date, date]:
    """First and last day of `ym` ("YYYY-MM"), or of the previous month."""
    if ym:
        y, m = (int(x) for x in ym.split("-"))
        first = date(y, m, 1)
    else:
        first = (datetime.now(PHT).date().replace(day=1) - timedelta(days=1)).replace(day=1)
    nxt = first.replace(year=first.year + 1, month=1) if first.month == 12 else first.replace(month=first.month + 1)
    return first, nxt - timedelta(days=1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Queue a previous-month daily backfill for every active station.")
    ap.add_argument("month", nargs="?", help="YYYY-MM (default: the previous month)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    date_from, date_to = month_bounds(args.month)
    if date_to >= datetime.now(PHT).date():
        sys.exit(f"{date_from:%Y-%m} is not over yet — today's row is a placeholder until tonight's sync")
    reason = f"monthly accuracy run: re-read {date_from:%B %Y} from Solis for every active station"
    request_id = str(uuid.uuid4())

    with db.connect(autocommit=True) as conn:
        rows = conn.execute(FLEET_TARGETS_SQL).fetchall()
    targets = [r for r in rows if not r["busy"]]
    log.info("%d active station(s); %d to queue, %d skipped (a daily job is already queued or running).",
             len(rows), len(targets), len(rows) - len(targets))
    if args.dry_run or not targets:
        return

    with db.audited(None, None, reason, source="monthly_refresh", request_id=request_id) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """insert into public.backfill_jobs
                     (system_id, solis_station_id, granularity, date_from, date_to,
                      requested_by, requested_reason, request_id)
                   values (%s, %s, 'daily', %s, %s, null, %s, %s)
                   on conflict do nothing""",
                [(r["id"], r["solis_station_id"], date_from, date_to, reason, request_id) for r in targets])
    log.info("Queued %d job(s) for %s → %s under request %s.", len(targets), date_from, date_to, request_id)


if __name__ == "__main__":
    main()
