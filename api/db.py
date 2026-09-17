"""
Direct Postgres access for Monitoring Admin and the workers.

Why not PostgREST for everything: the grid joins auth.users (not exposed over
PostgREST), and every mutation must carry WHO did it and WHY into the audit
trigger from migration 08. That trigger reads five transaction-local GUCs
(app.actor_id, app.actor_email, app.reason, app.request_id, app.source), and
PostgREST gives no way to set them in the same transaction as the write. A real
connection does, and it also gives real transactions for "edit the mapping AND
enqueue the backfill" as one unit.

Connections are POOLED. The API runs in Render's Oregon region and the database
in Mumbai (~230 ms each way); opening a fresh connection per request cost
~1 s of TLS + auth round trips before any query ran. A small pool keeps a few
warm connections for the life of the process.

Connection resolution, in order:
  1. SUPABASE_DB_POOLER_URL if set (Supabase session-mode pooler, IPv4).
  2. SUPABASE_DB_URL (the direct host, db.<ref>.supabase.co).
  3. Derived from SUPABASE_DB_URL's password: the ap-south-1 session pooler.
The direct host has ONLY an IPv6 address; a network without IPv6 fails on it,
and a container might too. The first DSN that connects is the one the pool is
built on, so only the very first request pays for a failed attempt.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import uuid
from typing import Iterator, List, Optional
from urllib.parse import quote, urlparse

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

PROJECT_REF = "kzsocvzhbgtfyksrjmvk"
# The project's region. Every other region's pooler answers "tenant not found".
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"

# Supabase's session pooler caps connections per role; four is plenty for a
# single-instance Monitoring Admin plus a worker, and leaves room for the crons.
POOL_MIN, POOL_MAX = 1, 4

_pool: Optional[ConnectionPool] = None
_lock = threading.Lock()


def _candidate_dsns() -> List[str]:
    dsns: List[str] = []
    pooler = os.getenv("SUPABASE_DB_POOLER_URL", "").strip()
    direct = os.getenv("SUPABASE_DB_URL", "").strip()
    if pooler:
        dsns.append(pooler)
    if direct:
        dsns.append(direct)
        if not pooler:
            u = urlparse(direct)
            if u.password:
                dsns.append(
                    f"postgresql://postgres.{PROJECT_REF}:{quote(u.password)}"
                    f"@{POOLER_HOST}:5432/postgres"
                )
    return dsns


def _pick_dsn() -> str:
    """Probe the candidates once and return the first that connects."""
    candidates = _candidate_dsns()
    if not candidates:
        raise RuntimeError("No database configured: set SUPABASE_DB_URL or SUPABASE_DB_POOLER_URL")
    last: Optional[Exception] = None
    for dsn in candidates:
        try:
            with psycopg.connect(dsn, connect_timeout=10):
                return dsn
        except psycopg.OperationalError as exc:
            last = exc
            log.warning("db connect via %s failed: %s", urlparse(dsn).hostname, str(exc).splitlines()[0][:140])
    raise RuntimeError("Database unreachable on every configured connection") from last


def _get_pool() -> ConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    with _lock:
        if _pool is None:
            dsn = _pick_dsn()
            _pool = ConnectionPool(
                dsn,
                min_size=POOL_MIN,
                max_size=POOL_MAX,
                kwargs={"row_factory": dict_row},
                timeout=15,
                # Recycle connections every 30 minutes so the pooler can rebalance
                # and a stale TLS session never lingers across a DB failover.
                max_lifetime=1800,
                open=True,
            )
            log.info("db pool opened via %s", urlparse(dsn).hostname)
    return _pool


@contextlib.contextmanager
def connect(autocommit: bool = False) -> Iterator[psycopg.Connection]:
    """A pooled connection. Commits on clean exit, rolls back on exception,
    and always returns the connection to the pool with autocommit reset."""
    pool = _get_pool()
    with pool.connection() as conn:
        conn.autocommit = autocommit
        try:
            yield conn
        finally:
            # Put autocommit back so the next borrower starts from the default.
            # ONLY after an autocommit block: psycopg refuses to change the flag
            # while a transaction is open, and in a transactional block the
            # caller's writes are still uncommitted at this point — the first
            # version reset unconditionally, raised, and the pool rolled the
            # whole transaction back. Every audited write returned "Database
            # error" (2026-09-17) while reads worked.
            if autocommit:
                conn.autocommit = False


@contextlib.contextmanager
def audited(
    actor_id: Optional[str],
    actor_email: Optional[str],
    reason: str,
    # audit_log.source. Rows written before the 2026-09-18 rename say
    # 'backoffice'; filter on both when reading history.
    source: str = "monitoring_admin",
    request_id: Optional[str] = None,
) -> Iterator[psycopg.Connection]:
    """One transaction whose writes the audit trigger attributes to `actor`.

    The third argument of set_config is `true` = transaction-local, so nothing
    leaks into the next user of the pooled connection. The pool's context
    manager commits on success and rolls back on any exception.
    """
    with connect(autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select set_config('app.actor_id',    %s, true),
                       set_config('app.actor_email', %s, true),
                       set_config('app.reason',      %s, true),
                       set_config('app.request_id',  %s, true),
                       set_config('app.source',      %s, true)
                """,
                (actor_id or "", actor_email or "", reason or "", str(request_id or uuid.uuid4()), source),
            )
        yield conn
