"""
Solviva worker — one always-on process for everything that used to be four
Render crons (solviva-five-minute-sync, solviva-sync, solviva-monthly-refresh,
solviva-backfill-worker).

    python -m api.worker                       # all tasks
    python -m api.worker --tasks jobs,health   # a subset (retire the crons one at a time)
    python -m api.worker --dry-run             # print what would be due now, check the DB, exit

Why one process (2026-10-04): four containers each held their own
SolisCloudClient, so the 10 req/s gate was per process and the hourly history
load next to the five-minute cron drew 429s from Solis; every five minutes a
container booted (~30–60 s, 288 times a day); and nothing watched the feeds —
the 2026-09-15 outage ran ~20 hours before a person noticed. Here: ONE Solis
client, so one gate for every task; no cold starts; each run recorded in
public.sync_runs (migration 23); a stale five-minute feed alerts within
FEED_STALE_MINUTES.

Tasks (Manila time):
  five_minute  every SYNC_CADENCE_MINUTES (5). A FULL fleet pass on slots
               divisible by FLEET_EVERY_MINUTES (15); in between only the "hot"
               stations — those a portal login viewed in the last
               HOT_STATION_MINUTES (60), from public.station_activity. The
               hourly roll-up rides on every pass (api.sync_five_minutes_to_supabase).
  nightly      NIGHTLY_AT (02:00): onboarding → daily readings → referral codes
               → identity mirror (api.sync_to_supabase.run_nightly).
  monthly      the 1st at MONTHLY_AT (09:00): queue a daily re-read of the
               previous month for every station (api.enqueue_fleet_backfill).
  jobs         drain public.backfill_jobs continuously (api.backfill_worker).
  health       every 5 minutes: age of the newest five-minute point; alert on
               the webhook/email when it exceeds FEED_STALE_MINUTES, again
               every 6 h while stale, and once on recovery.

A Postgres advisory lock makes sure only one worker runs. The process never
exits on a task failure: the failure is logged, recorded in sync_runs, and the
next slot runs. Render restarts the process if it does die.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import smtplib
import socket
import sys
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Awaitable, Callable, Dict, Optional, Set

from dotenv import load_dotenv

load_dotenv()

import psycopg  # noqa: E402
from supabase import create_client  # noqa: E402

from api import db  # noqa: E402
from api import backfill_worker as bw  # noqa: E402
from api.enqueue_fleet_backfill import enqueue_fleet, month_bounds  # noqa: E402
from api.onboard_from_odoo import _send_webhook  # noqa: E402
from api.solis_client import SolisCloudClient  # noqa: E402
from api.sync_five_minutes_to_supabase import sync_once as five_minute_pass  # noqa: E402
from api.sync_to_supabase import run_nightly  # noqa: E402

log = logging.getLogger("worker")
PHT = timezone(timedelta(hours=8))
HOST = socket.gethostname()

CADENCE_MINUTES = int(os.getenv("SYNC_CADENCE_MINUTES", "5"))
FLEET_EVERY_MINUTES = int(os.getenv("FLEET_EVERY_MINUTES", "15"))
HOT_STATION_MINUTES = int(os.getenv("HOT_STATION_MINUTES", "60"))
FEED_STALE_MINUTES = int(os.getenv("FEED_STALE_MINUTES", "30"))
NIGHTLY_AT = os.getenv("NIGHTLY_AT", "02:00")
MONTHLY_AT = os.getenv("MONTHLY_AT", "09:00")
ALL_TASKS = ("five_minute", "nightly", "monthly", "jobs", "health")
TICK_SECONDS = 20


def _env(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Missing env var: {key}")
    return v


def _hhmm(s: str) -> tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


# ---------------------------------------------------------------------------
# sync_runs
# ---------------------------------------------------------------------------
class Runs:
    """One row per task run in public.sync_runs."""

    def start(self, task: str, stats: Optional[Dict[str, Any]] = None) -> Optional[int]:
        try:
            with db.connect(autocommit=True) as conn:
                row = conn.execute(
                    "insert into public.sync_runs (task, host, stats) values (%s, %s, %s::jsonb) returning id",
                    (task, HOST, json.dumps(stats or {}, default=str))).fetchone()
            return int(row["id"])
        except Exception as exc:
            log.warning("sync_runs start failed: %s", str(exc)[:120])
            return None

    def finish(self, run_id: Optional[int], status: str, stats: Optional[Dict[str, Any]] = None,
               error: Optional[str] = None) -> None:
        if run_id is None:
            return
        try:
            with db.connect(autocommit=True) as conn:
                conn.execute(
                    """update public.sync_runs
                          set finished_at = now(), status = %s, error = %s,
                              stats = stats || %s::jsonb
                        where id = %s""",
                    (status, (error or None) and error[:900], json.dumps(stats or {}, default=str), run_id))
        except Exception as exc:
            log.warning("sync_runs finish failed: %s", str(exc)[:120])

    def close_interrupted(self) -> int:
        """Rows left at 'running' by a previous process (a redeploy kills the
        worker mid-pass). The advisory lock means no other worker is alive, so
        every 'running' row at start-up is stale."""
        try:
            with db.connect(autocommit=True) as conn:
                cur = conn.execute(
                    """update public.sync_runs
                          set status = 'failed', finished_at = now(),
                              error = coalesce(error, 'interrupted — the worker restarted before this run finished')
                        where status = 'running'""")
                n = cur.rowcount
            if n:
                log.info("closed %d interrupted run(s) from a previous worker process", n)
            return n
        except Exception as exc:
            log.warning("could not close interrupted runs: %s", str(exc)[:120])
            return 0

    def exists_since(self, task: str, since: datetime, statuses=("running", "succeeded")) -> bool:
        with db.connect(autocommit=True) as conn:
            row = conn.execute(
                "select 1 from public.sync_runs where task = %s and started_at >= %s and status = any(%s) limit 1",
                (task, since, list(statuses))).fetchone()
        return row is not None


# ---------------------------------------------------------------------------
# Alerts (webhook + email), reusing the onboarding alert settings
# ---------------------------------------------------------------------------
def _send_email_text(subject: str, text: str) -> None:
    recipients, host = os.getenv("ALERT_EMAIL_TO"), os.getenv("SMTP_HOST")
    if not recipients or not host:
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = os.getenv("ALERT_EMAIL_FROM") or os.getenv("SMTP_USER") or recipients.split(",")[0].strip()
        msg["To"] = recipients
        msg.set_content(text)
        with smtplib.SMTP(host, int(os.getenv("SMTP_PORT", "587")), timeout=20) as s:
            s.starttls()
            if os.getenv("SMTP_USER"):
                s.login(os.getenv("SMTP_USER", ""), os.getenv("SMTP_PASSWORD", ""))
            s.send_message(msg)
    except Exception as exc:
        log.error("alert email failed: %s", exc)


def alert(subject: str, text: str) -> None:
    log.warning("ALERT %s — %s", subject, text)
    _send_webhook(f"{subject} — {text}")
    _send_email_text(f"[Solviva] {subject}", text)


# ---------------------------------------------------------------------------
# The worker
# ---------------------------------------------------------------------------
class Worker:
    def __init__(self, tasks: Set[str], dry_run: bool = False):
        self.tasks = tasks
        self.dry_run = dry_run
        self.runs = Runs()
        self.stop = asyncio.Event()
        self.sb = create_client(_env("SUPABASE_URL"), _env("SUPABASE_SERVICE_KEY"))
        self.solis = SolisCloudClient(_env("SOLIS_CLOUD_KEY_ID"), _env("SOLIS_CLOUD_KEY_SECRET"))
        self.sem = asyncio.Semaphore(bw.SOLIS_CONCURRENCY)
        self._last_five_slot: Optional[datetime] = None
        self._last_health_slot: Optional[datetime] = None
        self._nightly_done: Optional[date] = None
        self._monthly_done: Optional[str] = None
        self._stale_since: Optional[datetime] = None
        self._last_stale_alert: Optional[datetime] = None

    # -- helpers ----------------------------------------------------------
    async def _run(self, task: str, factory: Callable[[], Awaitable[Any]],
                   stats: Optional[Dict[str, Any]] = None, alert_on_failure: bool = False) -> None:
        run_id = self.runs.start(task, stats)
        t0 = datetime.now(timezone.utc)
        try:
            result = await factory()
            out = {"seconds": round((datetime.now(timezone.utc) - t0).total_seconds(), 1)}
            if isinstance(result, dict):
                out.update(result)
            elif result is not None:
                out["rows"] = result
            self.runs.finish(run_id, "succeeded", out)
            log.info("%s done in %ss %s", task, out["seconds"], {k: v for k, v in out.items() if k != "seconds"})
        except Exception as exc:
            self.runs.finish(run_id, "failed", {"seconds": round((datetime.now(timezone.utc) - t0).total_seconds(), 1)},
                             f"{type(exc).__name__}: {exc}")
            log.exception("%s failed", task)
            if alert_on_failure:
                alert(f"{task} failed", f"{type(exc).__name__}: {str(exc)[:300]}")

    def _hot_stations(self) -> Set[str]:
        with db.connect(autocommit=True) as conn:
            rows = conn.execute(
                "select system_id::text as id from public.station_activity where last_viewed_at > now() - (%s || ' minutes')::interval",
                (str(HOT_STATION_MINUTES),)).fetchall()
        return {r["id"] for r in rows}

    @staticmethod
    def _slot(now: datetime, minutes: int) -> datetime:
        return now.replace(minute=(now.minute // minutes) * minutes, second=0, microsecond=0)

    # -- tasks ------------------------------------------------------------
    async def maybe_five_minute(self, now: datetime) -> None:
        slot = self._slot(now, CADENCE_MINUTES)
        if slot == self._last_five_slot:
            return
        self._last_five_slot = slot
        full = slot.minute % FLEET_EVERY_MINUTES == 0
        hot: Optional[Set[str]] = None
        if not full:
            hot = self._hot_stations()
            if not hot:
                log.info("%s: no station viewed in the last %d min — skipping the hot pass", slot.strftime("%H:%M"), HOT_STATION_MINUTES)
                return
        mode = {"mode": "fleet" if full else "hot", "stations": None if full else len(hot or ())}
        if self.dry_run:
            log.info("would run five_minute %s", mode)
            return
        await self._run("five_minute", lambda: five_minute_pass(solis=self.solis, only_system_ids=hot), mode)

    async def maybe_nightly(self, now: datetime) -> None:
        h, m = _hhmm(NIGHTLY_AT)
        if now.date() == self._nightly_done or (now.hour, now.minute) < (h, m):
            return
        # Catch-up window: a worker that (re)starts in the morning still runs a
        # missed nightly, but one started in the afternoon does not re-run a
        # chain the cron era already did (deploy day) — tomorrow's 02:00 is soon.
        if now.hour >= 12:
            self._nightly_done = now.date()
            return
        day_start = datetime.combine(now.date(), datetime.min.time(), tzinfo=PHT)
        if self.runs.exists_since("nightly", day_start):      # already ran today (e.g. before a restart)
            self._nightly_done = now.date()
            return
        self._nightly_done = now.date()
        if self.dry_run:
            log.info("would run nightly")
            return
        await self._run("nightly", lambda: run_nightly(self.solis, self.sb), alert_on_failure=True)

    async def maybe_monthly(self, now: datetime) -> None:
        h, m = _hhmm(MONTHLY_AT)
        key = now.strftime("%Y-%m")
        if now.day != 1 or key == self._monthly_done or (now.hour, now.minute) < (h, m):
            return
        month_start = datetime.combine(now.date().replace(day=1), datetime.min.time(), tzinfo=PHT)
        if self.runs.exists_since("monthly", month_start):
            self._monthly_done = key
            return
        self._monthly_done = key
        if self.dry_run:
            log.info("would run monthly")
            return
        d0, d1 = month_bounds(None)

        async def go():
            return await asyncio.to_thread(
                enqueue_fleet, "daily", d0, d1,
                f"monthly accuracy run: re-read {d0:%B %Y} from Solis for every active station", "monthly_refresh")
        await self._run("monthly", go, alert_on_failure=True)

    async def maybe_health(self, now: datetime) -> None:
        slot = self._slot(now, 5)
        if slot == self._last_health_slot:
            return
        self._last_health_slot = slot
        try:
            with db.connect(autocommit=True) as conn:
                row = conn.execute("select max(last_ts) as last_ts from public.five_minute_watermarks").fetchone()
        except Exception as exc:
            log.warning("health check could not read the watermark view: %s", str(exc)[:120])
            return
        last_ts = row["last_ts"]
        # Right after Manila midnight the view is empty until the first run of
        # the day has written; that is not staleness.
        just_after_midnight = (now.hour, now.minute) < (0, FEED_STALE_MINUTES)
        age_min = None if last_ts is None else (now - last_ts.astimezone(PHT)).total_seconds() / 60
        stale = (age_min is None and not just_after_midnight) or (age_min is not None and age_min > FEED_STALE_MINUTES)
        if stale:
            if self._stale_since is None:
                self._stale_since = now
            if self._last_stale_alert is None or (now - self._last_stale_alert) > timedelta(hours=6):
                self._last_stale_alert = now
                text = (f"newest five-minute point is {age_min:.0f} min old" if age_min is not None
                        else "no five-minute rows for today") + f" (threshold {FEED_STALE_MINUTES} min)"
                self.runs.finish(self.runs.start("health"), "failed", {"age_minutes": age_min}, text)
                if not self.dry_run:
                    alert("five-minute feed stale", text)
        elif self._stale_since is not None:
            down = now - self._stale_since
            self._stale_since = None
            self._last_stale_alert = None
            self.runs.finish(self.runs.start("health"), "succeeded", {"recovered_after_minutes": round(down.total_seconds() / 60)})
            if not self.dry_run:
                alert("five-minute feed recovered", f"after {down.total_seconds() / 60:.0f} min")

    async def jobs_loop(self) -> None:
        if self.dry_run:
            # Claiming flips a job to 'running'; a dry run must not touch the queue.
            with db.connect(autocommit=True) as conn:
                n = conn.execute("select count(*) as n from public.backfill_jobs where status = 'queued'").fetchone()["n"]
            log.info("would drain %d queued job(s)", n)
            return
        while not self.stop.is_set():
            job = None
            try:
                with db.audited(None, None, "claimed by worker", source="backfill_worker") as conn:
                    job = bw.claim_next(conn)
            except Exception as exc:
                log.warning("job claim failed: %s", str(exc)[:120])
            if job is None:
                await self._sleep(15)
                continue
            try:
                await bw.process(job, self.sb, self.solis, self.sem)
            except Exception:
                log.exception("job %s crashed outside process()", job.get("id"))

    # -- loop ---------------------------------------------------------------
    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def ticker(self) -> None:
        while not self.stop.is_set():
            now = datetime.now(PHT)
            for name, fn in (("five_minute", self.maybe_five_minute), ("nightly", self.maybe_nightly),
                             ("monthly", self.maybe_monthly), ("health", self.maybe_health)):
                if name in self.tasks:
                    try:
                        await fn(now)
                    except Exception:
                        log.exception("%s tick failed", name)
            if self.dry_run:
                return
            await self._sleep(TICK_SECONDS)

    async def run(self) -> None:
        log.info("worker starting on %s: tasks=%s cadence=%d fleet_every=%d hot=%d min stale=%d min nightly=%s monthly=%s",
                 HOST, ",".join(sorted(self.tasks)), CADENCE_MINUTES, FLEET_EVERY_MINUTES, HOT_STATION_MINUTES,
                 FEED_STALE_MINUTES, NIGHTLY_AT, MONTHLY_AT)
        if not self.dry_run:
            self.runs.close_interrupted()
        coros = [self.ticker()]
        if "jobs" in self.tasks:
            coros.append(self.jobs_loop())
        try:
            await asyncio.gather(*coros)
        finally:
            await self.solis.aclose()
            log.info("worker stopped")


def _hold_lock() -> psycopg.Connection:
    """One worker at a time: a session-level advisory lock on a dedicated
    connection, held for the life of the process."""
    conn = psycopg.connect(db._pick_dsn(), autocommit=True)
    ok = conn.execute("select pg_try_advisory_lock(hashtext('solviva-worker'))").fetchone()[0]
    if not ok:
        conn.close()
        sys.exit("another worker already holds the solviva-worker lock")
    return conn


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Solviva always-on worker")
    ap.add_argument("--tasks", default=os.getenv("WORKER_TASKS", ",".join(ALL_TASKS)),
                    help="comma-separated subset of: " + ",".join(ALL_TASKS))
    ap.add_argument("--dry-run", action="store_true", help="say what is due now, touch nothing, exit")
    args = ap.parse_args()
    tasks = {t.strip() for t in args.tasks.split(",") if t.strip()}
    unknown = tasks - set(ALL_TASKS)
    if unknown:
        sys.exit(f"unknown task(s): {', '.join(sorted(unknown))}")

    lock_conn = _hold_lock()
    worker = Worker(tasks, dry_run=args.dry_run)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, worker.stop.set)
        except (NotImplementedError, RuntimeError):      # Windows
            signal.signal(sig, lambda *_: worker.stop.set())
    try:
        await worker.run()
    finally:
        lock_conn.close()


if __name__ == "__main__":
    asyncio.run(main())
