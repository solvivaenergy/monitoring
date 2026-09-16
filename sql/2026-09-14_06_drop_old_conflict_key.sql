-- 2026-09-14 step 06 — DROP the single-station unique key.
-- Run this once in the Supabase SQL editor. Order: AFTER file 05 AND AFTER the
-- code deploy described at the end of file 05. Re-runnable (a second run finds
-- nothing to drop).
--
-- This is the single line that permits two stations of one user to both hold a
-- row on the same day. Nothing before it actually delivers multi-station.
--
-- The constraint name is not knowable from outside the database (PostgREST
-- exposes no catalog), so this discovers it by definition rather than guessing.
-- It handles both shapes: a UNIQUE constraint, and a bare unique index with no
-- constraint behind it. It will not touch the new (system_id, "timestamp") keys.
--
-- *** HEAVY LOCK WARNING: DROP CONSTRAINT takes ACCESS EXCLUSIVE, but it is a
-- catalog operation and completes in milliseconds. ***
--
-- DO NOT RUN THIS UNTIL THE CODE IS DEPLOYED. While the old
-- on_conflict="user_id,timestamp" string is live, dropping this key makes every
-- backfill and both crons fail with 42P10.

do $$
declare
  r record;
  t text;
begin
  foreach t in array array['public.energy_readings',
                           'public.energy_readings_five_minutes']
  loop
    -- (a) UNIQUE constraints defined exactly on (user_id, "timestamp")
    for r in
      select c.conname
        from pg_constraint c
       where c.conrelid = t::regclass
         and c.contype  = 'u'
         and pg_get_constraintdef(c.oid) ~* '^unique \(user_id, "?timestamp"?\)$'
    loop
      execute format('alter table %s drop constraint %I', t, r.conname);
      raise notice 'dropped constraint %.%', t, r.conname;
    end loop;

    -- (b) bare unique indexes on the same columns with no constraint behind them
    for r in
      select i.indexrelid::regclass::text as idx
        from pg_index i
       where i.indrelid = t::regclass
         and i.indisunique
         and not i.indisprimary
         and not exists (select 1 from pg_constraint c where c.conindid = i.indexrelid)
         and pg_get_indexdef(i.indexrelid) ~* '\(user_id, "?timestamp"?\)\s*$'
    loop
      execute format('drop index if exists %s', r.idx);
      raise notice 'dropped index %', r.idx;
    end loop;
  end loop;
end $$;

-- Confirm the old key is gone and the new one is present.
select t.relname, c.conname, pg_get_constraintdef(c.oid)
  from pg_constraint c join pg_class t on t.oid = c.conrelid
 where t.relname in ('energy_readings','energy_readings_five_minutes')
   and c.contype = 'u';

-- ------------------------------------------------------------------
-- Dropping that key removed user_id's ONLY index on both tables. Every portal
-- query still filters by user, so replace it with a plain (non-unique) one.
-- *** RUN EACH OF THESE TWO ALONE — CONCURRENTLY. ***
-- ------------------------------------------------------------------
create index concurrently if not exists energy_readings_user_ts_idx
  on public.energy_readings (user_id, "timestamp");

create index concurrently if not exists energy_readings_5m_user_ts_idx
  on public.energy_readings_five_minutes (user_id, "timestamp");
