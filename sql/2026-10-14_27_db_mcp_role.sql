-- Migration 27 — read-only role for the "Solviva Databases" MCP connector.
--
-- WHY. One claude.ai connector (db_mcp.py, Render service solviva-db-mcp) gives
-- the whole Claude Teams org read access to this database without a Supabase
-- login each. It connects as db_mcp_ro, a role that can only SELECT, so the
-- connector cannot write even if asked to.
--
-- Same shape as inquiry_ro (2026-09-20, the solviva-inquiry folder's role) but
-- a separate role on purpose: retiring that folder (`drop role inquiry_ro`)
-- must not take the team connector down, and pg_stat_activity tells the two
-- apart.
--
--   login, password set at apply time (never committed), connection limit 3
--   default_transaction_read_only = on  — belt; the grants below are the braces
--   bypassrls                           — every public table has RLS on; without
--                                         it the role would see empty tables
--   SELECT on every current table and view in public, and on inquiry.auth_users
--   ALTER DEFAULT PRIVILEGES for postgres — tables created later in public are
--     readable too, so a new migration need not remember this role
--
-- HOW TO RUN: as postgres (Management API query endpoint or the direct
-- connection), after replacing <PASSWORD>. Re-runnable; re-running rotates
-- the password. ROLLBACK:
--   drop owned by db_mcp_ro; drop role db_mcp_ro;

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'db_mcp_ro') then
    create role db_mcp_ro;
  end if;
end $$;
-- ///
alter role db_mcp_ro with login password '<PASSWORD>' connection limit 3 bypassrls nocreatedb nocreaterole;
-- ///
alter role db_mcp_ro set default_transaction_read_only = on;
-- ///
alter role db_mcp_ro set statement_timeout = '60s';
-- ///
grant usage on schema public to db_mcp_ro;
-- ///
grant select on all tables in schema public to db_mcp_ro;
-- ///
alter default privileges for role postgres in schema public grant select on tables to db_mcp_ro;
-- ///
grant usage on schema inquiry to db_mcp_ro;
-- ///
grant select on inquiry.auth_users to db_mcp_ro;
-- ///
