-- 2026-09-14 step 05 — ADD the new per-station unique key.
-- Run this once in the Supabase SQL editor. Order: after 04.
--
-- *** THIS FILE MUST RUN BEFORE THE CODE DEPLOY. FILE 06 MUST RUN AFTER IT. ***
--
-- Sequencing, and why it is not optional:
--   05 (add new key)  ->  DEPLOY CODE  ->  06 (drop old key)
-- If the old key is dropped while on_conflict="user_id,timestamp" is still
-- deployed, PostgREST raises 42P10 "there is no unique or exclusion constraint
-- matching the ON CONFLICT specification" and the nightly sync fails loudly and
-- completely. Loud is the SAFE failure — but the deploy window is real.
-- Running 05 first means both keys coexist, so old and new code both work and
-- the deploy has no outage.
--
-- THE NEW CONFLICT KEY IS (system_id, "timestamp").
--
-- Not (user_id, system_id, timestamp): system_id already functionally
-- determines user_id (0 cross-linked rows in 114,858), so adding user_id buys
-- nothing and re-introduces a collision if a station is ever reassigned — which
-- is precisely what the back office is being built to do.
--
-- Not (system_id, reading_date) — REJECTING the data-integrity audit's
-- recommendation. 405 legacy rows (0.35%, created 2026-02-28/03-26/03-27) carry
-- insert-time wall-clock timestamps instead of the canonical 04:00:00Z. They
-- form 357 (user_id, calendar-date) groups holding 2-3 rows each, and I checked
-- every one: 343 are canonical+legacy, 14 are legacy+legacy, and 337 of the 357
-- hold GENUINELY DIFFERENT production_kwh values. A date-truncated key would
-- silently destroy one real value in 337 groups. File 12 quarantines them
-- separately if you want date-granularity later.
--
-- NOTE ON 04:00:00Z: it is 12:00 NOON Asia/Manila, not midnight. 114,453 of
-- 114,858 rows encode noon. Never "fix" this by shifting to 16:00Z.
--
-- WHAT THIS MEANS FOR THE EXISTING 114,858 ROWS: nothing. Zero rows are
-- rewritten, zero timestamps normalised. Verified by simulation after the file
-- 03 merge: duplicate (system_id, timestamp) pairs = 0 on energy_readings
-- (114,858 rows) and 0 on energy_readings_five_minutes (129,423 rows). The
-- index applies cleanly on both.
--
-- *** HEAVY LOCK WARNING ***
-- The two CREATE UNIQUE INDEX CONCURRENTLY statements take SHARE UPDATE
-- EXCLUSIVE on 114k/129k-row tables that are written continuously.
-- RUN EACH ONE ALONE, as the sole contents of the editor.
-- The subsequent ALTER TABLE ... ADD CONSTRAINT ... USING INDEX takes
-- ACCESS EXCLUSIVE but only for milliseconds, because the index already exists
-- — that is the whole point of building it concurrently first.
--
-- Indexes are plain ASC, deliberately. The schema-perf audit's DDL specified
-- ("timestamp" desc); I am not using it. ON CONFLICT arbiter inference is
-- load-bearing here (a mismatch kills every backfill with 42P10), and a plain
-- ASC btree serves ORDER BY "timestamp" DESC LIMIT 1 identically via a backward
-- index scan. DESC would buy nothing and add risk.

-- (1 of 2) RUN ALONE
create unique index concurrently if not exists energy_readings_system_ts_uk
  on public.energy_readings (system_id, "timestamp");

-- (2 of 2) RUN ALONE
create unique index concurrently if not exists energy_readings_5m_system_ts_uk
  on public.energy_readings_five_minutes (system_id, "timestamp");

-- Confirm both are VALID before going further. Any row returned here means a
-- CONCURRENTLY build failed and left a dead index that silently does nothing;
-- drop it concurrently and retry that statement.
select c.relname as invalid_index
  from pg_class c join pg_index i on i.indexrelid = c.oid
 where not i.indisvalid;

-- Promote the indexes to named constraints. Optional for PostgREST (ON CONFLICT
-- infers from a unique INDEX just as well) but it makes the invariant visible
-- to psql \d and to the next engineer. Millisecond ACCESS EXCLUSIVE each.
do $$
begin
  if not exists (select 1 from pg_constraint
                  where conrelid = 'public.energy_readings'::regclass
                    and conname  = 'energy_readings_system_ts_uk') then
    alter table public.energy_readings
      add constraint energy_readings_system_ts_uk
      unique using index energy_readings_system_ts_uk;
  end if;

  if not exists (select 1 from pg_constraint
                  where conrelid = 'public.energy_readings_five_minutes'::regclass
                    and conname  = 'energy_readings_5m_system_ts_uk') then
    alter table public.energy_readings_five_minutes
      add constraint energy_readings_5m_system_ts_uk
      unique using index energy_readings_5m_system_ts_uk;
  end if;
end $$;

-- ========================================================================
-- STOP HERE. DEPLOY THE CODE CHANGE NOW, THEN RUN FILE 06.
--
-- The six files below form ONE transitive dependency closure and must ship as
-- a SINGLE atomic deploy, because backfill_history.sb_batch_upsert is the
-- shared write path for three of them:
--
--   api/backfill_history.py:160-161        on_conflict "user_id,timestamp"
--                                            -> "system_id,timestamp"
--   api/backfill_history.py:225-232        system_ids = {} keyed by user_id
--                                            -> key by station/system, ORDER BY,
--                                               and PAGINATE (the unordered fetch
--                                               returns all 719 rows only because
--                                               PostgREST caps at 1000)
--   api/backfill_newly_onboarded.py:245,437  same on_conflict
--   api/backfill_newly_onboarded.py:150,310  same dict-keyed-by-user_id
--   api/backfill_newly_onboarded.py:152-172,312-333
--                                          "skip users who already have readings"
--                                            is user-scoped -> station 2 can never
--                                            be backfilled. Scope it to system_id.
--   api/repair_jul_aug_fast.py:127         same on_conflict
--   api/sync_five_minutes_to_supabase.py:424,438  same on_conflict
--                                            AND delete the duplicate upsert block
--                                            at :421-427 (the same batch is upserted
--                                            twice; removing the first, retry-less
--                                            loop halves insert traffic and removes
--                                            ~150k dead tuples/day — the cheapest
--                                            fix in the whole audit)
--   api/sync_five_minutes_to_supabase.py:217,390  existing_by_user dedup keyed by
--                                            user -> key by (system_id, ts)
--   api/sync_to_supabase.py:311-324        the existence probe selects
--                                            .eq("user_id").gte(day).lt(day) with NO
--                                            system_id predicate, then UPDATEs by id.
--                                            This is a SEPARATE bug from the
--                                            on_conflict one and would still clobber
--                                            station A with station B AFTER this
--                                            constraint change. Add .eq("system_id").
--   api/sync_to_supabase.py:208-215        .eq("user_id").eq("status","active")
--                                            .limit(1) -> .eq("solis_station_id", sid)
--   api/sync_to_supabase.py:216-227,247    _oldest_reading_date() is user-scoped and
--                                            writes back into installation_date; scope
--                                            it to system_id or a 2024 array will pull
--                                            a 2026 array's backfill window
--
-- Also, before the next onboarding batch: make _ensure_active_system
-- (sync_five_minutes_to_supabase.py:266) and the insert at
-- sync_to_supabase.py:261-266 idempotent upserts. File 04's
-- solar_systems_user_station_uk now makes the race fail loudly instead of
-- breeding duplicates, which means those two crons will start ERRORING on the
-- next onboarding night if they are not fixed.
-- ========================================================================
