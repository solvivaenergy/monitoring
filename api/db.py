"""
Direct Postgres access for the back office.

Why not PostgREST for everything: the grid joins auth.users (not exposed over
PostgREST), and every mutation must carry WHO did it and WHY into the audit
trigger from migration 08. That trigger reads five transaction-local GUCs
(app.actor_id, app.actor_email, app.reason, app.request_id, app.source), and
PostgREST gives no way to set them in the same transaction as the write. A real
connection does, and it also gives real transactions for "edit the mapping AND
enqueue the backfill" as one unit.

Connection resolution, in order:
  1. SUPABASE_DB_POOLER_URL if set (Supabase session-mode pooler, IPv4).
  2. SUPABASE_DB_URL (the direct host, db.<ref>.supabase.co).
  3. Derived from SUPABASE_DB_URL's password: the ap-south-1 session pooler.
The direct host has ONLY an IPv6 address; a network without IPv6 fails on it,
and a container might too. The first DSN that connects is remembered for the
process so only the first request pays for a failed attempt.
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

log = logging.getLogger(__name__)

PROJECT_REF = "kzsocvzhbgtfyksrjmvk"
# The project's region. Every other region's pooler answers "tenant not found".
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"

_working_dsn: Optional[str] = None
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


def connect(autocommit: bool = False) -> psycopg.Connection:
    """Open a connection, trying each candidate DSN once and remembering the winner."""
    global _working_dsn
    candidates = [_working_dsn] if _working_dsn else _candidate_dsns()
    if not candidates:
        raise RuntimeError("No database configured: set SUPABASE_DB_URL or SUPABASE_DB_POOLER_URL")

    last: Optional[Exception] = None
    for dsn in candidates:
        try:
            conn = psycopg.connect(dsn, autocommit=autocommit, connect_timeout=10, row_factory=dict_row)
            with _lock:
                _working_dsn = dsn
            return conn
        except psycopg.OperationalError as exc:
            last = exc
            log.warning("db connect via %s failed: %s", urlparse(dsn).hostname, str(exc).splitlines()[0][:140])

    if _working_dsn and len(candidates) == 1:
        # The remembered DSN stopped working; forget it and try the full list once.
        with _lock:
            _working_dsn = None
        return connect(autocommit=autocommit)
    raise RuntimeError("Database unreachable on every configured connection") from last


@contextlib.contextmanager
def audited(
    actor_id: Optional[str],
    actor_email: Optional[str],
    reason: str,
    source: str = "backoffice",
    request_id: Optional[str] = None,
) -> Iterator[psycopg.Connection]:
    """One transaction whose writes the audit trigger attributes to `actor`.

    The third argument of set_config is `true` = transaction-local, so nothing
    leaks into the next user of a pooled connection. Commit on success,
    rollback on any exception, always close.
    """
    conn = connect(autocommit=False)
    try:
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
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
