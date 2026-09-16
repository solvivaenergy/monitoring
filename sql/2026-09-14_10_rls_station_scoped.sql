-- 2026-09-14 step 10 — re-point customer RLS from the user to the station.
-- Run this once in the Supabase SQL editor. Order: after 09. Re-runnable.
-- No heavy locks (CREATE/DROP POLICY takes a brief ACCESS EXCLUSIVE, catalog-only).
--
-- *** RUN QUERY (d) IN FILE 00 FIRST AND SAVE THE OUTPUT. ***
-- This file DROPS every existing SELECT policy on the four customer-facing
-- tables and replaces them with one canonical policy each. That is deliberate —
-- you want exactly one readable rule per table, not an accumulated pile — but
-- it is irreversible without the record from file 00.
--
-- WHY THE CHANGE: a `user_id = auth.uid()` policy still PERMITS the portal's
-- queries after multi-station, but it stops being the thing that BOUNDS them. A
-- portal bug that drops `.eq('system_id', ...)` would silently merge both of a
-- customer's houses into one chart, and the database would happily allow it.
-- Make the grant itself station-derived so the authorization boundary and the
-- display boundary are the same boundary.
--
-- IMPLEMENTATION NOTE: the policy calls a SECURITY DEFINER helper rather than
-- inlining `exists (select 1 from solar_systems ...)`. A subquery inside a
-- policy is ITSELF subject to the referenced table's RLS — a classic footgun
-- that works today only by coincidence and breaks the moment solar_systems'
-- policy changes. The helper sidesteps it and is STABLE, so the planner caches it.

create or replace function public.owns_system(p_system_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select exists (
    select 1 from public.solar_systems s
     where s.id = p_system_id and s.user_id = auth.uid()
  );
$$;
revoke execute on function public.owns_system(uuid) from public, anon;
grant  execute on function public.owns_system(uuid) to authenticated;

-- Drop the existing SELECT policies on the four tables.
do $$
declare r record;
begin
  for r in
    select tablename, policyname
      from pg_policies
     where schemaname = 'public'
       and tablename in ('energy_readings','energy_readings_five_minutes',
                         'solar_systems','user_profiles')
       and cmd in ('SELECT','ALL')
  loop
    execute format('drop policy if exists %I on public.%I', r.policyname, r.tablename);
    raise notice 'dropped policy %.%', r.tablename, r.policyname;
  end loop;
end $$;

-- RLS must be on. (It already is on all four — anon and a throwaway
-- authenticated account both read */0 from every base table.)
alter table public.energy_readings              enable row level security;
alter table public.energy_readings_five_minutes enable row level security;
alter table public.solar_systems                enable row level security;
alter table public.user_profiles                enable row level security;

-- Canonical read policies. service_role bypasses RLS entirely, so every cron,
-- every script in api/, and the back-office service are unaffected by all of
-- this and need no policy.
do $$
begin
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='energy_readings' and policyname='er_read_own_station') then
    create policy er_read_own_station on public.energy_readings
      for select to authenticated using (public.owns_system(system_id));
  end if;

  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='energy_readings_five_minutes' and policyname='er5m_read_own_station') then
    create policy er5m_read_own_station on public.energy_readings_five_minutes
      for select to authenticated using (public.owns_system(system_id));
  end if;

  -- REQUIRED for the station picker: without this the portal's
  -- "which stations do I own?" query returns [] for everyone.
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='solar_systems' and policyname='ss_read_own') then
    create policy ss_read_own on public.solar_systems
      for select to authenticated using (user_id = auth.uid());
  end if;

  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='user_profiles' and policyname='up_read_own') then
    create policy up_read_own on public.user_profiles
      for select to authenticated using (id = auth.uid());
  end if;

  -- Staff read-everything, for a back office that talks to PostgREST directly.
  -- Harmless if it does not (service_role never consults policies).
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='solar_systems' and policyname='ss_read_staff') then
    create policy ss_read_staff on public.solar_systems
      for select to authenticated using (public.is_staff());
  end if;
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='user_profiles' and policyname='up_read_staff') then
    create policy up_read_staff on public.user_profiles
      for select to authenticated using (public.is_staff());
  end if;
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='energy_readings' and policyname='er_read_staff') then
    create policy er_read_staff on public.energy_readings
      for select to authenticated using (public.is_staff());
  end if;
end $$;

-- Deliberately NO insert/update/delete policies for anon or authenticated on
-- any of these tables. Probed live: anon and authenticated can write nothing
-- today, and customers cannot even edit their own profile through PostgREST.
-- Note what that probe ALSO showed: POST with body {} returns
-- 42501 "new row violates row-level security policy" rather than
-- "permission denied for table" — which proves the INSERT/UPDATE/DELETE GRANTs
-- are present for both roles and that RLS POLICIES ARE THE ONLY THING between
-- the public anon key and the whole database. One `USING (true)` policy, or one
-- `alter table ... disable row level security`, is total public write access.
-- Keep all writes on the server side of the FastAPI service.

-- Verify: all four must return 0 rows for anon.
select tablename, policyname, cmd, roles, qual
  from pg_policies
 where schemaname = 'public'
   and tablename in ('energy_readings','energy_readings_five_minutes',
                     'solar_systems','user_profiles','staff_users',
                     'audit_log','backfill_jobs')
 order by tablename, policyname;
