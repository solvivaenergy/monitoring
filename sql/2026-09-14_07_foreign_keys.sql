-- 2026-09-14 step 07 — the missing foreign keys.  APPLIED 2026-09-16 (as-run).
-- Order: after 02 (which repairs the orphans these would otherwise reject) and
-- after 06. Re-runnable. Run over the direct connection (autocommit).
--
-- THIS FILE WAS CORRECTED ON THE DAY IT RAN. The first draft failed with
--   42703: column "system_id" referenced in foreign key constraint does not exist
-- on billing_records, and the whole DO block rolled back. Two of its premises
-- were wrong, and the catalog settles both:
--
--   * "user_id is unconstrained everywhere" — FALSE. Every user_id column
--     (solar_systems, energy_readings, energy_readings_five_minutes,
--     billing_records, support_tickets, ticket_messages, energy_tips) already
--     has a FK to auth.users(id). What was missing is the guarantee that the
--     login also has a PROFILE — a login without a user_profiles row is exactly
--     how the 3 profile-less accounts (demo@, test@, one Aboitiz contact) and
--     the nightly flip-flop orphans came about. So the FKs below point at
--     user_profiles(id), and only on the three tables the back office and the
--     syncs actually write.
--   * billing_records has no system_id column; referrals has no user_id
--     (it is referrer_user_id and is already constrained). Both removed.
--
-- Precondition verified live before running: 0 rows in solar_systems,
-- energy_readings or energy_readings_five_minutes whose user_id lacks a
-- user_profiles row. All four constraints VALIDATED on first run.
--
-- *** LOCK NOTE ***
-- ADD CONSTRAINT ... FOREIGN KEY normally takes ACCESS EXCLUSIVE on BOTH tables
-- AND does a full validation scan under that lock. NOT VALID avoids the scan;
-- VALIDATE CONSTRAINT then runs under SHARE UPDATE EXCLUSIVE, which does not
-- block reads or writes. Every FK below uses that two-step. Do not collapse it.
--
-- ON DELETE RESTRICT, not CASCADE — deliberately. There is no soft delete and
-- no point-in-time recovery on this project. With CASCADE, deleting one
-- user_profiles row would cascade through solar_systems into 116k+ readings
-- with no confirmation and no way back. RESTRICT forces the back office to deal
-- with the history explicitly. CASCADE is used only where the child row is
-- pure metadata (staff_users, audit actor refs) in file 08.
--
-- cleaned_data.system_id had no FK at all; it has 0 rows today, so it validates
-- trivially. Its user_id gets none: the column is not written by anything live.

do $$
declare
  r record;
begin
  for r in
    select * from (values
      ('solar_systems',                'solar_systems_user_profile_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('energy_readings',              'energy_readings_user_profile_fkey',
       'user_id',   'public.user_profiles(id)'),
      ('energy_readings_five_minutes', 'energy_readings_5m_user_profile_fkey',
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

    -- VALIDATE separately; SHARE UPDATE EXCLUSIVE only. Left to raise on
    -- failure: a FK that cannot validate means orphan rows exist, and the fix
    -- is file 02's repair, not a quieter migration.
    if exists (select 1 from pg_constraint
                where conrelid = ('public.' || r.tbl)::regclass
                  and conname  = r.con and not convalidated) then
      execute format('alter table public.%I validate constraint %I', r.tbl, r.con);
      raise notice 'validated %', r.con;
    end if;
  end loop;
end $$;

-- Any FK left NOT VALID has offending rows. Find them before re-running.
select conrelid::regclass as tbl, conname, convalidated
  from pg_constraint
 where contype = 'f' and not convalidated
   and connamespace = 'public'::regnamespace;
-- Expect 0 rows.
