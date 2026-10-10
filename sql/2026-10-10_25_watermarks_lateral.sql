-- Migration 25 — five_minute_watermarks without the sort and the temp files.
--
-- WHY. The view from migration 17 (re-created by 21 after the partitioning)
-- materialised today's five-minute rows into a CTE, read it twice (DISTINCT ON
-- for the latest point, GROUP BY for the count) and sorted ~93k rows with
-- work_mem = 5 MB. EXPLAIN ANALYZE on 2026-10-10: "Sort Method: external
-- merge Disk: 3096kB", temp written 855 blocks (~6.8 MB), 249 ms. It is read
-- ~300 times a day — the five-minute sync's watermark request, the worker's
-- health check every 5 minutes, Monitoring Admin's Health tab — and
-- pg_stat_statements since 2026-09-01 showed it as the largest producer of
-- temp-file writes in the database (18.8 GB + 8.6 GB). The cost grows with
-- the fleet: at 3,000 stations it would be ~30 MB of temp per call.
--
-- WHAT. Same four columns, same rows (every station with at least one row in
-- the current Asia/Manila day), produced as two bounded index scans per
-- station on each partition's unique (system_id, "timestamp") index: the
-- latest point is an ORDER BY ... DESC LIMIT 1 walk, the count an index range.
-- Nothing is sorted, nothing spills. Measured before applying: identical
-- output to the old definition, ~30 ms, temp 0. Stations are enumerated from
-- solar_systems; a five-minute row cannot exist without its solar_systems row
-- (migration 07's foreign key), so the row set is the same.
--
-- Invoker rights are re-asserted (CREATE OR REPLACE VIEW replaces reloptions)
-- and the grants from 17/21 re-applied so the lock-down does not depend on
-- what the replace keeps: anon/authenticated nothing, service_role and
-- inquiry_ro SELECT.
--
-- HOW TO RUN: direct Postgres connection, autocommit, one statement at a time
-- (`-- ///` markers). Re-runnable. No code change: the sync, the worker and
-- the Health tab keep reading the same columns.

create or replace view public.five_minute_watermarks
with (security_invoker = on) as
select s.id              as system_id,
       l."timestamp"     as last_ts,
       l.lifetime_earning as last_lifetime_earning,
       c.n               as n_rows
  from public.solar_systems s
  cross join lateral (
    select f."timestamp", f.lifetime_earning
      from public.energy_readings_five_minutes f
     where f.system_id = s.id
       and f."timestamp" >= (date_trunc('day', now() at time zone 'Asia/Manila') at time zone 'Asia/Manila')
     order by f."timestamp" desc
     limit 1) l
  cross join lateral (
    select count(*) as n
      from public.energy_readings_five_minutes f
     where f.system_id = s.id
       and f."timestamp" >= (date_trunc('day', now() at time zone 'Asia/Manila') at time zone 'Asia/Manila')) c;
-- ///
revoke all on public.five_minute_watermarks from anon, authenticated;
-- ///
grant select on public.five_minute_watermarks to service_role, inquiry_ro;
-- ///
notify pgrst, 'reload schema';
-- ///
-- Verify: reloptions carry security_invoker, no temp blocks in the plan, and
-- the same station count / newest point / rows-today as before the change.
select c.relname, c.reloptions from pg_class c where c.relname = 'five_minute_watermarks';
-- ///
explain (analyze, buffers) select * from public.five_minute_watermarks;
-- ///
select count(*) as stations, max(last_ts) as newest, sum(n_rows) as rows_today
  from public.five_minute_watermarks;

-- ROLLBACK: the migration-21 definition.
--   create or replace view public.five_minute_watermarks with (security_invoker = on) as
--   with today as (
--     select system_id, "timestamp", lifetime_earning
--       from public.energy_readings_five_minutes
--      where "timestamp" >= (date_trunc('day', now() at time zone 'Asia/Manila') at time zone 'Asia/Manila')
--   )
--   select l.system_id, l."timestamp" as last_ts, l.lifetime_earning as last_lifetime_earning, c.n as n_rows
--     from (select distinct on (system_id) system_id, "timestamp", lifetime_earning
--             from today order by system_id, "timestamp" desc) l
--     join (select system_id, count(*) as n from today group by system_id) c using (system_id);
--   revoke all on public.five_minute_watermarks from anon, authenticated;
--   grant select on public.five_minute_watermarks to service_role, inquiry_ro;
--   notify pgrst, 'reload schema';
