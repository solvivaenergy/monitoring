-- 2026-09-14 step 07 — the missing foreign keys.
-- Run this once in the Supabase SQL editor. Order: after 02 (which repairs the
-- orphans these would otherwise reject) and after 06. Re-runnable.
--
-- Correcting the task premise: it is NOT true that there are "no FKs today".
-- energy_readings.system_id and energy_readings_five_minutes.system_id are both
-- real, NOT NULL FKs to solar_systems.id (proven by PostgREST embedding probe
-- and by the OpenAPI <fk/> annotation). It is user_id that is unconstrained
-- everywhere, and it has already drifted: 2 solar_systems rows and 637
-- energy_readings rows pointed at user_profiles ids that do not exist.
--
-- *** HEAVY LOCK WARNING ***
-- ADD CONSTRAINT ... FOREIGN KEY normally takes ACCESS EXCLUSIVE on BOTH tables
-- AND does a full validation scan under that lock. NOT VALID avoids the scan;
-- VALIDATE CONSTRAINT then runs under SHARE UPDATE EXCLUSIVE, which does not
-- block reads or writes. Every FK below uses that two-step. Do not collapse it.
--
-- ON DELETE RESTRICT, not CASCADE — deliberately overriding the security
-- audit's suggestion. That audit's own blast-radius section is the argument:
-- there is no soft delete and no point-in-time recovery on this project. With
-- CASCADE, deleting one user_profiles row would cascade through solar_systems
-- into 114k+ readings with no confirmation and no way back. RESTRICT forces the
-- back office to deal with the history explicitly. CASCADE is used only where
-- the child row is pure metadata (staff_users, audit actor refs) in file 08.
--
-- cleaned_data gets NO user_id FK: it has 470 rows whose user_id is not in
-- user_profiles. (The data-integrity audit reported "2" — that was 2 DISTINCT
-- USERS, not 2 rows. Verified live: 470 rows.) Its system_id has 0 orphans, so
-- that one FK is viable and is included.

do $$
declare
  r record;
begin
  for r in
    select * from (values
      ('solar_systems',                'solar_systems_user_id_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('energy_readings',              'energy_readings_user_id_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('energy_readings_five_minutes', 'energy_readings_5m_user_id_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('billing_records',              'billing_records_user_id_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('billing_records',              'billing_records_system_id_fkey',
       'system_id', 'public.solar_systems(id)'),
      ('support_tickets',              'support_tickets_user_id_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('referrals',                    'referrals_user_id_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('cleaned_data',                 'cleaned_data_system_id_fkey',
       'system_id', 'public.solar_systems(id)')
    ) as v(tbl, con, col, ref)
  loop
    if not exists (select 1 from pg_constraint
                    where conrelid = ('public.' || r.tbl)::regclass
                      and conname  = r.con) then
      execute format(
        'alter table public.%I add constraint %I foreign key (%I) references %s on delete restrict not valid',
        r.tbl, r.con, r.col, r.ref);
      raise notice 'added % (NOT VALID)', r.con;
    end if;

    -- VALIDATE separately; SHARE UPDATE EXCLUSIVE only.
    if exists (select 1 from pg_constraint
                where conrelid = ('public.' || r.tbl)::regclass
                  and conname  = r.con and not convalidated) then
      begin
        execute format('alter table public.%I validate constraint %I', r.tbl, r.con);
        raise notice 'validated %', r.con;
      exception when others then
        raise warning 'could not validate % : % — fix the offending rows and re-run', r.con, sqlerrm;
      end;
    end if;
  end loop;
end $$;

-- Any FK left NOT VALID has offending rows. Find them before re-running.
select conrelid::regclass as tbl, conname, convalidated
  from pg_constraint
 where contype = 'f' and not convalidated
   and connamespace = 'public'::regnamespace;
