-- 2026-09-14 step 01 — missing indexes. No behaviour change, safe to run now.
-- Run this once in the Supabase SQL editor. Order: after 00, before 02.
--
-- Why: the schema-perf audit measured (interleaved latency, n=40, network floor
-- 109.1 ms) that energy_readings(system_id) IS indexed but "timestamp" is NOT on
-- either reading table. _purge_old_rows() in api/sync_five_minutes_to_supabase.py
-- :228-233 runs DELETE ... WHERE "timestamp" < cutoff on every one of its 96 daily
-- runs -> 96 sequential scans/day of a ~129k-row table. This file kills that.
--
-- *** HEAVY LOCK WARNING ***
-- Every statement below is CREATE INDEX CONCURRENTLY. It takes only
-- SHARE UPDATE EXCLUSIVE (writes keep working) but it CANNOT RUN INSIDE A
-- TRANSACTION BLOCK. The Supabase SQL editor may wrap a multi-statement
-- submission in one transaction.
-- => RUN EACH STATEMENT BELOW ON ITS OWN, as the sole contents of the editor.
-- If one fails part-way it leaves an INVALID index that silently does nothing;
-- check with query (f) in file 00 and drop+retry.
--
-- If your editor refuses CONCURRENTLY entirely, drop the word CONCURRENTLY.
-- solar_systems is 719 rows (<50 ms); the two reading tables are 114k/129k and
-- should still be done in a cron gap (daily sync 18:00 UTC; the 15-min cron
-- leaves ~13 idle minutes each quarter hour).

-- (1 of 4) 5-minute purge + "load today's window". 96 seq scans/day -> 0.
create index concurrently if not exists energy_readings_five_minutes_timestamp_idx
  on public.energy_readings_five_minutes ("timestamp");

-- (2 of 4) fleet-health "any reading since cutoff" (solviva_mcp.py:562) and
--          global max(timestamp).
create index concurrently if not exists energy_readings_timestamp_idx
  on public.energy_readings ("timestamp");

-- (3 of 4) solar_systems.user_id is the join column of every back-office page
--          and of the new station picker. There is no index on it today.
create index concurrently if not exists solar_systems_user_id_idx
  on public.solar_systems (user_id);

-- (4 of 4) the syncs' "all active stations" predicate. Note status is 'active'
--          for all 719 rows today, so this is a no-op filter until file 04
--          introduces a real lifecycle; the index is correct either way.
create index concurrently if not exists solar_systems_active_idx
  on public.solar_systems (status) where status = 'active';
