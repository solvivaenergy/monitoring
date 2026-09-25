-- 2026-09-25 step 17 — cut write amplification on the two reading tables, and
-- give the five-minute sync a one-request watermark view.
--
-- Trigger: Supabase's "running out of Disk IO Budget" mail of 2026-09-23. The
-- project runs on NANO compute (0.5 GB RAM, 5 MB/s / 250 IOPS baseline). The
-- Observability → Database report shows ~400 MB of SWAP in use around the
-- clock and 1.62 GB committed against a 0.86 GB commit limit: the box is
-- swapping, and that — not queries — is what eats the IO budget. The compute
-- upgrade (Nano → Small) is the fix for that; this file removes the write
-- waste that we DO control, measured over the 22 days since the counters were
-- reset on 2026-09-01:
--
--   * energy_readings_five_minutes carries SIX indexes: 57 MB of index on an
--     11 MB heap. Three are dead weight —
--       energy_readings_five_minutes_user_id_timestamp_idx   13 MB, 0 scans
--       energy_readings_five_minutes_system_id_timestamp_idx 12 MB, duplicate of
--                                                            energy_readings_5m_system_ts_uk (same columns; a btree
--                                                            is scanned backwards for ORDER BY … DESC)
--       energy_readings_5m_user_ts_idx                        9.5 MB, 72 scans
--     Every insert maintains all six, and autovacuum (462 runs in 22 days,
--     ~21/day because the table is rewritten daily) scans every index each time.
--     RLS on this table goes through owns_system(system_id), not user_id, and
--     the only user_id lookups are Monitoring Admin's retire/merge counts —
--     a seq scan of ~80k rows, ~10 ms.
--   * energy_readings likewise:
--       idx_energy_readings_user_timestamp    7.4 MB, 0 scans
--       idx_energy_readings_system_timestamp  7.5 MB, duplicate of energy_readings_system_ts_uk
--     energy_readings_user_ts_idx is KEPT (1,720 scans; Monitoring Admin's
--     merge/retire paths filter the 121k-row table by user_id).
--   * Only 111 of 447 "latest point" refreshes per run were HOT updates: the
--     pages are packed (fillfactor 100), so 3 of 4 refreshes move the row and
--     touch every index. fillfactor 85 leaves room for the one in-place update
--     each row gets. The daily table gets its current-month rows rewritten
--     nightly, so it gets 90.
--   * The sync used to page through EVERY row of the day (~64 PostgREST
--     requests per run, hint-bit dirtying on all of them) just to learn what it
--     already had. five_minute_watermarks answers that in one request:
--     per station, the latest timestamp today, its lifetime_earning, and how
--     many rows today. 58 ms measured. Same lock-down as migration 11's views:
--     invoker rights, anon/authenticated revoked, service role only.
--
-- HOW TO RUN: over the direct Postgres connection with autocommit, one
-- statement at a time (the `-- ///` markers are for scratchpad/dbrun.py).
-- DROP INDEX CONCURRENTLY cannot run inside a transaction block (25001), and
-- the SQL editor wraps every submission in one. Do NOT drop the CONCURRENTLY:
-- both tables are written every 15 minutes.
--
-- Order: apply BEFORE deploying the code that reads the view. The old code is
-- indifferent to everything here. Re-runnable.

drop index concurrently if exists public.energy_readings_five_minutes_user_id_timestamp_idx;
-- ///
drop index concurrently if exists public.energy_readings_five_minutes_system_id_timestamp_idx;
-- ///
drop index concurrently if exists public.energy_readings_5m_user_ts_idx;
-- ///
drop index concurrently if exists public.idx_energy_readings_user_timestamp;
-- ///
drop index concurrently if exists public.idx_energy_readings_system_timestamp;
-- ///
-- Catalog-only; applies to pages written from now on. The five-minute table is
-- rewritten every day, so it is fully on the new setting by tomorrow.
alter table public.energy_readings_five_minutes set (fillfactor = 85);
-- ///
alter table public.energy_readings set (fillfactor = 90);
-- ///
-- Today's watermark per station, Asia/Manila day. Rows older than today only
-- exist while a purge is pending, and must not count.
create or replace view public.five_minute_watermarks
with (security_invoker = on) as
with today as (
  select system_id, "timestamp", lifetime_earning
    from public.energy_readings_five_minutes
   where "timestamp" >= (date_trunc('day', now() at time zone 'Asia/Manila') at time zone 'Asia/Manila')
)
select l.system_id,
       l."timestamp"      as last_ts,
       l.lifetime_earning as last_lifetime_earning,
       c.n                as n_rows
  from (select distinct on (system_id) system_id, "timestamp", lifetime_earning
          from today
         order by system_id, "timestamp" desc) l
  join (select system_id, count(*) as n from today group by system_id) c
    using (system_id);
-- ///
revoke all on public.five_minute_watermarks from anon, authenticated;
-- ///
grant select on public.five_minute_watermarks to service_role;
-- ///
-- Supabase's pgrst_ddl_watch event trigger reloads PostgREST's schema cache on
-- DDL; this is belt and braces so the view is visible to the very next run.
notify pgrst, 'reload schema';
-- ///
-- Verify: 3 indexes left on the five-minute table, 4 on the daily table,
-- reloptions set, view answers.
select c.relname, c.reloptions,
       (select count(*) from pg_index i where i.indrelid = c.oid) as indexes
  from pg_class c
 where c.relname in ('energy_readings', 'energy_readings_five_minutes');
-- ///
select count(*) as stations, max(last_ts) as newest, sum(n_rows) as rows_today
  from public.five_minute_watermarks;

-- ROLLBACK (if a query turns out to need one of these; each alone, CONCURRENTLY):
--   create index concurrently energy_readings_five_minutes_user_id_timestamp_idx
--     on public.energy_readings_five_minutes (user_id, "timestamp" desc);
--   create index concurrently energy_readings_five_minutes_system_id_timestamp_idx
--     on public.energy_readings_five_minutes (system_id, "timestamp" desc);
--   create index concurrently energy_readings_5m_user_ts_idx
--     on public.energy_readings_five_minutes (user_id, "timestamp");
--   create index concurrently idx_energy_readings_user_timestamp
--     on public.energy_readings (user_id, "timestamp" desc);
--   create index concurrently idx_energy_readings_system_timestamp
--     on public.energy_readings (system_id, "timestamp" desc);
--   alter table public.energy_readings_five_minutes reset (fillfactor);
--   alter table public.energy_readings reset (fillfactor);
--   drop view if exists public.five_minute_watermarks;
