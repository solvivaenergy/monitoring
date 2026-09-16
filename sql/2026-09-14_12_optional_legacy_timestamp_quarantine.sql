-- 2026-09-14 step 12 — OPTIONAL. Quarantine the 405 legacy off-grid readings.
-- Run this once in the Supabase SQL editor. Order: any time after 05.
-- Re-runnable. NOT a prerequisite for anything. Read the whole header first.
--
-- 99.647% of energy_readings (114,453 of 114,858) sit at exactly 04:00:00Z,
-- which is 12:00 NOON Asia/Manila — the canonical value produced by
-- api/sync_to_supabase.py:290. The other 405 are legacy seed rows whose
-- timestamp carries the insert-time now() of a one-off script:
--   created_at 2026-03-27 (256 rows), 2026-03-26 (140), 2026-02-28 (9)
-- Times scattered across 13:00 (116), 01:00 (65), 16:00 (43), etc.
--
-- CONSEQUENCE: 357 (user_id, calendar-date) groups already hold 2-3 rows — the
-- same day stored twice. UNIQUE(user_id, timestamp) never caught them because
-- the TIMES differ, and UNIQUE(system_id, timestamp) from file 05 does not
-- either. So the new key is correct and idempotent for every code path that
-- exists, but the table is not strictly one-row-per-station-per-day.
--
-- I characterised all 357 groups before recommending anything:
--     343 groups = one canonical 04:00Z row + one legacy row
--      14 groups = two legacy rows, no canonical row at all
--     337 of 357 hold DIFFERENT production_kwh values between their rows
--     350 groups have 2 rows, 7 have 3
--      27 of the 405 legacy rows are NOT in any multi-row group (they are the
--         only row for their day and must be KEPT and canonicalised, not deleted)
--
-- That 337 is why file 05 does NOT use a date-granularity key and why this file
-- is optional. Collapsing these to one row per day is a JUDGEMENT CALL that
-- discards a real, differing number in 337 cases. Do it only if the team
-- decides the canonical 04:00Z row is authoritative — which is plausible, since
-- the legacy rows come from a seeding script that predates the current sync.
--
-- STEP A — inspect. Always run this first; do not skip to step B.
select r.user_id, r.system_id, r."timestamp", r.production_kwh,
       r.consumption_kwh, r.created_at,
       (r."timestamp"::time = time '04:00:00') as canonical
  from public.energy_readings r
  join (select user_id, ("timestamp" at time zone 'UTC')::date as d
          from public.energy_readings
         group by 1, 2 having count(*) > 1) g
    on g.user_id = r.user_id
   and g.d = ("timestamp" at time zone 'UTC')::date
 order by r.user_id, r."timestamp";

-- STEP B — export before deleting anything. Download this as CSV and keep it
-- alongside the repo's existing report files
-- (odoo_prod_vs_xlsx_station_audit_*.json etc.).
create table if not exists public.energy_readings_legacy_quarantine
  (like public.energy_readings including defaults);

insert into public.energy_readings_legacy_quarantine
select r.*
  from public.energy_readings r
 where r."timestamp"::time <> time '04:00:00'
   and exists (select 1 from public.energy_readings x
                where x.user_id = r.user_id
                  and ("timestamp" at time zone 'UTC')::date
                      = (r."timestamp" at time zone 'UTC')::date
                  and x.id <> r.id
                  and x."timestamp"::time = time '04:00:00')
on conflict do nothing;

select count(*) as quarantined from public.energy_readings_legacy_quarantine;
-- expect ~348 (the legacy row of each of the 343 mixed groups, plus the extras
-- in the 7 three-row groups). The 14 all-legacy groups and the 27 lone legacy
-- rows are NOT captured here, deliberately — there is no canonical row to
-- prefer, so they need a human decision.

-- STEP C — delete the quarantined rows from the live table.
-- *** DESTRUCTIVE. Only after step B has been exported and reviewed. ***
-- delete from public.energy_readings r
--  using public.energy_readings_legacy_quarantine q
--  where r.id = q.id;

-- STEP D — canonicalise the 27 lone legacy rows to 04:00Z so the table becomes
-- uniformly one-row-per-station-per-day. Safe: they collide with nothing.
-- *** Run only after C. ***
-- update public.energy_readings
--    set "timestamp" = date_trunc('day', "timestamp" at time zone 'UTC')
--                      + interval '4 hours'
--  where "timestamp"::time <> time '04:00:00'
--    and not exists (select 1 from public.energy_readings x
--                     where x.user_id = energy_readings.user_id
--                       and x.id <> energy_readings.id
--                       and ("timestamp" at time zone 'UTC')::date
--                           = (energy_readings."timestamp" at time zone 'UTC')::date);

-- STEP E — once A-D leave zero duplicate (system_id, date) groups, you MAY
-- tighten the key. Only then.
-- select count(*) from (
--   select system_id, ("timestamp" at time zone 'UTC')::date
--     from public.energy_readings group by 1,2 having count(*) > 1) q;
