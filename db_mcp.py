"""
Solviva Databases MCP server — read-only SQL over Solviva's Supabase projects.

One connector for the whole Claude Teams org ("Solviva Databases" in claude.ai),
built like solviva_mcp.py — shared service token + Anthropic IP allowlist, no
per-user Supabase login — and serving four Postgres databases through one
SELECT-only role:

    monitoring           kzsocvzhbgtfyksrjmvk  "Mobile App Solis Data": customer logins,
                                               solar systems, daily / hourly / five-minute
                                               readings, audit log, staff, onboarding runs
    calculator           gbwfhacvklwieqnydqzb  Internal Calculator (production): pricing
                                               parameters, devices, tiers, promo codes, roles
    calculator_staging   xifuuxtsdyajcbyksjxx  Internal Calculator (Staging)
    calculator_testing   azljcxncghmncvwrphdq  Internal Calculator (Testing)

Tools: list_projects, query, list_tables, describe_table.

Why it cannot write, in layers (each one alone would do):
  1. The Postgres role `db_mcp_ro` (one per project; sql/2026-10-14_27_db_mcp_role.sql
     here, supabase/migrations/20261014_create_db_mcp_ro_role.sql in
     InternalCalcBackEnd) holds SELECT only. No INSERT, UPDATE, DELETE, DDL.
     It defaults every transaction to READ ONLY and has BYPASSRLS because every
     public table has RLS on (without it the role would see empty tables).
  2. Every call here runs inside an explicit READ ONLY transaction that is
     always rolled back, with a 60 s statement timeout.
  3. A query must be ONE statement (no `;`) starting with SELECT, WITH, TABLE,
     VALUES, EXPLAIN or SHOW. `SET transaction_read_only = off`, `DELETE`,
     `CREATE` and friends are refused before they reach the database.
  4. SELECT-shaped queries stream through a server-side cursor and stop at
     `limit` (max 2000) rows, so `select * from energy_readings_hourly` cannot
     exhaust the container's memory.

Environment (Render: service solviva-db-mcp, see render.yaml; locally: .env):
    DB_MCP_SERVICE_TOKEN     shared bearer token (falls back to MCP_SERVICE_TOKEN)
    MCP_PUBLIC_HOST          public hostname, for DNS-rebinding protection
    DB_MCP_MONITORING_URL    postgresql://db_mcp_ro.<ref>:<pw>@<session pooler>:5432/postgres
    DB_MCP_CALC_URL          same shape, one per project. An unset project is
    DB_MCP_CALC_STAGING_URL  listed as not configured and its calls fail with a
    DB_MCP_CALC_TESTING_URL  clear message instead of a crash.

Run locally (stdio, for Claude Code):      python db_mcp.py
Run as the remote server (Render):         python db_mcp.py --http --port 8082
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
from typing import Any, Callable, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from mcp_auth import BearerAuthMiddleware

try:  # local runs read .env next to this file; Render injects the environment
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=False)
except ImportError:
    pass

DEFAULT_ROWS = 200
MAX_ROWS = 2000
STATEMENT_TIMEOUT = "60s"
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)

PROJECTS: Dict[str, Dict[str, str]] = {
    "monitoring": {
        "env": "DB_MCP_MONITORING_URL",
        "ref": "kzsocvzhbgtfyksrjmvk",
        "title": "Mobile App Solis Data (monitoring)",
        "about": (
            "Customer portal / mobile app data. user_profiles (one row per login; "
            "odoo_lead_id, solis_station_id mirrors), solar_systems (one row per plant: "
            "solis_station_id, capacity_kwp, status, electricity_provider_id), "
            "energy_readings (one row per plant per day; timestamp 04:00Z = noon Manila; "
            "today is a zero placeholder until the 02:00 PHT sync), energy_readings_hourly, "
            "energy_readings_five_minutes (rolling live curve), audit_log (occurred_at, "
            "actor_email, table_name, row_pk, field_name, old/new_value), backfill_jobs, "
            "onboarding_runs, sync_runs, staff_users, system_access, the "
            "monthly_energy_sync* views, and inquiry.auth_users (login email, created_at, "
            "last_sign_in_at; join user_profiles.id). Timestamps are UTC: use "
            "`at time zone 'Asia/Manila'`."
        ),
    },
    "calculator": {
        "env": "DB_MCP_CALC_URL",
        "ref": "gbwfhacvklwieqnydqzb",
        "title": "Internal Calculator (production)",
        "about": (
            "Pricing-calculator configuration: app_parameters (the single live parameter "
            "row), devices, device_settings, inverters, battery_packages, panel_settings, "
            "cabling_tiers, mounting_support, roof_material_rates, rsd_pricing, "
            "variable_charges, fixed_overhead, location_delivery_charges, interest_rates, "
            "promo_codes, quote_settings, schedule_constants, pricing_meta, "
            "maintenance_mode, user_roles (calculator logins and roles), "
            "parameter_audit_events."
        ),
    },
    "calculator_staging": {
        "env": "DB_MCP_CALC_STAGING_URL",
        "ref": "xifuuxtsdyajcbyksjxx",
        "title": "Internal Calculator (Staging)",
        "about": "Same schema as calculator; the staging site's data.",
    },
    "calculator_testing": {
        "env": "DB_MCP_CALC_TESTING_URL",
        "ref": "azljcxncghmncvwrphdq",
        "title": "Internal Calculator (Testing)",
        "about": "Same schema as calculator; the isolated testing environment (2026-09-30).",
    },
}

CURSOR_VERBS = {"select", "with", "table", "values"}
DIRECT_VERBS = {"explain", "show"}

mcp = MCPServer(
    name="solviva-db",
    instructions=(
        "Read-only SQL over Solviva's Supabase Postgres databases. Pick a project "
        "with `project`: monitoring (customer portal: logins, solar systems, daily / "
        "hourly / five-minute energy readings, audit log), calculator (Internal "
        "Calculator production: pricing parameters, devices, promo codes, user roles), "
        "calculator_staging, calculator_testing. Call list_projects for what each "
        "holds, then list_tables / describe_table before guessing column names. One "
        "SELECT-shaped statement per query call; writes are impossible at every "
        "layer. Times are stored in UTC — show Manila with `at time zone 'Asia/Manila'`; "
        "in energy_readings, 04:00:00Z is noon Manila. Customer rows hold personal "
        "data: aggregate when the answer allows it."
    ),
    version="0.1.0",
)


# --------------------------------------------------------------------------
# Connections: one per project, serialised by a lock, reconnected once on a
# dropped session. The session pooler is used because the direct db.<ref>
# host is IPv6-only and Render containers have no IPv6 route.
# --------------------------------------------------------------------------


class _Db:
    def __init__(self, key: str, dsn: str) -> None:
        self.key = key
        self.dsn = dsn
        self.lock = threading.Lock()
        self.conn: Optional[psycopg.Connection] = None

    def _connect(self) -> psycopg.Connection:
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(
                self.dsn,
                connect_timeout=15,
                row_factory=dict_row,
                application_name="solviva-db-mcp",
            )
        return self.conn

    def _drop(self) -> None:
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass
        self.conn = None

    def run(self, work: Callable[[psycopg.Connection], Dict[str, Any]]) -> Dict[str, Any]:
        """Run `work(conn)` inside a READ ONLY transaction that is always rolled back."""
        with self.lock:
            last_error: Optional[Exception] = None
            for _attempt in (1, 2):
                try:
                    conn = self._connect()
                except psycopg.Error as exc:
                    last_error = exc
                    self._drop()
                    continue
                try:
                    with conn.cursor() as cur:
                        cur.execute("set transaction read only")
                        cur.execute(f"set local statement_timeout = '{STATEMENT_TIMEOUT}'")
                    return work(conn)
                except psycopg.Error as exc:
                    # A server-side error (bad SQL, permission denied, statement
                    # timeout 57014) carries a SQLSTATE: report it. Only a
                    # dropped session (no SQLSTATE, or class 08 connection
                    # exceptions) is worth one reconnect-and-retry.
                    state = exc.sqlstate or ""
                    if isinstance(exc, psycopg.OperationalError) and (not state or state.startswith("08")):
                        last_error = exc
                        self._drop()
                        print(
                            f"[{self.key}] reconnecting after {str(exc).splitlines()[0][:120]}",
                            file=sys.stderr,
                            flush=True,
                        )
                        continue
                    return _pg_error(exc)
                finally:
                    try:
                        if self.conn is not None and not self.conn.closed:
                            self.conn.rollback()
                    except Exception:
                        self._drop()
            return {"error": f"database unreachable: {str(last_error)[:200]}"}


_dbs: Dict[str, _Db] = {}


def _db(project: str) -> _Db:
    if project not in PROJECTS:
        raise ValueError(f"unknown project {project!r}; one of: {', '.join(PROJECTS)}")
    if project not in _dbs:
        dsn = os.getenv(PROJECTS[project]["env"], "").strip()
        if not dsn:
            raise ValueError(
                f"project {project!r} is not configured on this server "
                f"({PROJECTS[project]['env']} is unset)"
            )
        _dbs[project] = _Db(project, dsn)
    return _dbs[project]


def _pg_error(exc: psycopg.Error) -> Dict[str, Any]:
    diag = exc.diag
    return {
        "error": diag.message_primary or str(exc),
        "sqlstate": exc.sqlstate,
        "detail": diag.message_detail,
        "hint": diag.message_hint,
    }


def _rows_payload(cur, rows: List[Dict[str, Any]], limit: int) -> Dict[str, Any]:
    truncated = len(rows) > limit
    rows = rows[:limit]
    payload: Dict[str, Any] = {
        "columns": [d.name for d in cur.description] if cur.description else [],
        "rows": json.loads(json.dumps(rows, default=str)),
        "row_count": len(rows),
        "truncated": truncated,
    }
    if truncated:
        payload["note"] = f"cut at {limit} rows; add ORDER BY/LIMIT or raise limit (max {MAX_ROWS})"
    return payload


def _run_params(project: str, query: str, params: tuple, limit: int = MAX_ROWS) -> Dict[str, Any]:
    """Internal catalogue queries (parameterised, trusted SQL)."""

    def work(conn: psycopg.Connection) -> Dict[str, Any]:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return _rows_payload(cur, cur.fetchmany(limit + 1), limit)

    try:
        return _db(project).run(work)
    except ValueError as exc:
        return {"error": str(exc)}


# --------------------------------------------------------------------------
# User SQL
# --------------------------------------------------------------------------


def _strip_leading_comments(text: str) -> str:
    text = text.lstrip()
    while True:
        if text.startswith("--"):
            nl = text.find("\n")
            text = "" if nl < 0 else text[nl + 1 :].lstrip()
        elif text.startswith("/*"):
            end = text.find("*/")
            text = "" if end < 0 else text[end + 2 :].lstrip()
        else:
            return text


def _classify(sql: str):
    """Return (clean_sql, verb) or (None, error message)."""
    text = _strip_leading_comments(sql or "")
    text = text.rstrip().rstrip(";").rstrip()
    if not text:
        return None, "sql must not be empty"
    if ";" in text:
        return None, (
            "one statement per call: a ';' is not accepted (if it is inside a string "
            "literal, use chr(59) instead)"
        )
    match = re.match(r"[A-Za-z]+", text)
    verb = match.group(0).lower() if match else ""
    if verb not in CURSOR_VERBS | DIRECT_VERBS:
        return None, (
            f"only SELECT / WITH / TABLE / VALUES / EXPLAIN / SHOW statements run here "
            f"(got {verb.upper() or text[:20]!r}); this connector is read-only"
        )
    return text, verb


def _run_user(project: str, sql: str, limit: int) -> Dict[str, Any]:
    limit = max(1, min(int(limit), MAX_ROWS))
    text, verb = _classify(sql)
    if text is None:
        return {"error": verb}

    if verb in CURSOR_VERBS:

        def work(conn: psycopg.Connection) -> Dict[str, Any]:
            # Server-side cursor: Postgres materialises nothing beyond what we
            # fetch, so a table-sized SELECT costs limit+1 rows of memory here.
            with conn.cursor(name="db_mcp_q", scrollable=False, withhold=False) as cur:
                cur.itersize = limit + 1
                cur.execute(text)
                return _rows_payload(cur, cur.fetchmany(limit + 1), limit)

    else:

        def work(conn: psycopg.Connection) -> Dict[str, Any]:
            with conn.cursor() as cur:
                cur.execute(text)
                return _rows_payload(cur, cur.fetchmany(limit + 1), limit)

    try:
        return _db(project).run(work)
    except ValueError as exc:
        return {"error": str(exc)}


# --------------------------------------------------------------------------
# Tools
# --------------------------------------------------------------------------


@mcp.tool(
    description=(
        "The databases this server can read: project key (use it as `project` in the "
        "other tools), Supabase ref, what it holds, and whether it is configured here."
    ),
    annotations=READ_ONLY,
)
def list_projects() -> Dict[str, Any]:
    return {
        "projects": [
            {
                "project": key,
                "title": meta["title"],
                "supabase_ref": meta["ref"],
                "configured": bool(os.getenv(meta["env"], "").strip()),
                "about": meta["about"],
            }
            for key, meta in PROJECTS.items()
        ]
    }


@mcp.tool(
    description=(
        "Run ONE read-only SQL statement (SELECT / WITH / TABLE / VALUES / EXPLAIN / SHOW) "
        "on a project's database and return the rows. Runs in a READ ONLY transaction "
        "with a 60 s timeout as a SELECT-only role; writes are impossible. `limit` caps "
        "the rows returned (default 200, max 2000) — add ORDER BY and LIMIT yourself for "
        "anything bigger. Timestamps are UTC: use `at time zone 'Asia/Manila'`. Use "
        "list_tables / describe_table first when unsure of names."
    ),
    annotations=READ_ONLY,
)
def query(project: str, sql: str, limit: int = DEFAULT_ROWS) -> Dict[str, Any]:
    return _run_user(project, sql, limit)


@mcp.tool(
    description=(
        "Tables and views of one schema in a project (default public) with approximate "
        "row counts and comments. Only what the read-only role may read is listed; on "
        "monitoring the schema 'inquiry' holds the auth_users view."
    ),
    annotations=READ_ONLY,
)
def list_tables(project: str, schema: str = "public") -> Dict[str, Any]:
    return _run_params(
        project,
        """
        select c.relname as name,
               case c.relkind when 'r' then 'table' when 'p' then 'partitioned table'
                              when 'v' then 'view' when 'm' then 'materialized view' end as kind,
               coalesce(s.n_live_tup, 0) as approx_rows,
               obj_description(c.oid) as comment
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        left join pg_stat_user_tables s on s.relid = c.oid
        where n.nspname = %s and c.relkind in ('r', 'p', 'v', 'm')
          and has_table_privilege(c.oid, 'SELECT')
        order by c.relkind, c.relname
        """,
        (schema,),
    )


@mcp.tool(
    description=(
        "Columns (type, nullable, default, comment), constraints (primary / foreign / "
        "unique / check) and indexes of one table or view in a project."
    ),
    annotations=READ_ONLY,
)
def describe_table(project: str, table: str, schema: str = "public") -> Dict[str, Any]:
    columns = _run_params(
        project,
        """
        select column_name, data_type, is_nullable = 'YES' as nullable, column_default,
               col_description((quote_ident(%s) || '.' || quote_ident(%s))::regclass, ordinal_position) as comment
        from information_schema.columns
        where table_schema = %s and table_name = %s
        order by ordinal_position
        """,
        (schema, table, schema, table),
    )
    if "error" in columns:
        return columns
    if not columns["rows"]:
        return {"error": f"{schema}.{table} not found or not readable"}
    constraints = _run_params(
        project,
        """
        select conname as name,
               case contype when 'p' then 'primary key' when 'f' then 'foreign key'
                            when 'u' then 'unique' when 'c' then 'check' end as kind,
               pg_get_constraintdef(oid) as definition
        from pg_constraint
        where conrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass
        order by contype, conname
        """,
        (schema, table),
    )
    indexes = _run_params(
        project,
        "select indexname as name, indexdef as definition from pg_indexes "
        "where schemaname = %s and tablename = %s order by 1",
        (schema, table),
    )
    return {
        "project": project,
        "table": f"{schema}.{table}",
        "columns": columns["rows"],
        "constraints": constraints.get("rows", []),
        "indexes": indexes.get("rows", []),
    }


# --------------------------------------------------------------------------
# Entry point — same shape as solviva_mcp.main
# --------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Solviva Databases MCP server (read-only).")
    parser.add_argument("--http", action="store_true", help="Serve streamable HTTP instead of stdio.")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8082")))
    parser.add_argument(
        "--allow-any-ip",
        action="store_true",
        help="Skip the Anthropic IP allowlist (local testing only).",
    )
    args = parser.parse_args()

    configured = [k for k, m in PROJECTS.items() if os.getenv(m["env"], "").strip()]
    print(f"[db_mcp] projects configured: {', '.join(configured) or 'none'}", file=sys.stderr, flush=True)

    if not args.http:
        mcp.run(transport="stdio")
        return

    token = os.getenv("DB_MCP_SERVICE_TOKEN", "") or os.getenv("MCP_SERVICE_TOKEN", "")
    if not token:
        raise SystemExit("DB_MCP_SERVICE_TOKEN must be set when serving over HTTP.")

    import uvicorn
    from mcp.server.transport_security import TransportSecuritySettings

    public_host = os.getenv("MCP_PUBLIC_HOST", "")
    if public_host:
        security = TransportSecuritySettings(
            allowed_hosts=[public_host, f"{public_host}:*"],
            allowed_origins=[f"https://{public_host}"],
        )
    else:
        security = TransportSecuritySettings(enable_dns_rebinding_protection=False)

    app = mcp.streamable_http_app(stateless_http=True, transport_security=security)
    restrict = not args.allow_any_ip and os.getenv("MCP_RESTRICT_IPS", "true").lower() != "false"
    if not restrict:
        print("[auth] IP allowlist DISABLED — bearer token is the only control.", flush=True)
    app = BearerAuthMiddleware(app, token=token, restrict_ips=restrict)
    uvicorn.run(app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
