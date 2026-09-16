-- ============================================================================
-- FILE 04 IS SPLIT INTO THREE PARTS: 04a -> 04b -> 04c. Run them in order.
--
-- Why: the Supabase SQL editor runs a submission inside a transaction, and
-- CREATE INDEX CONCURRENTLY cannot run inside one (ERROR 25001). 04b holds
-- every CONCURRENTLY statement and must be run ONE STATEMENT AT A TIME; 04a
-- and 04c contain none and paste whole.
-- ============================================================================

-- Run 04a and 04b FIRST. This file contains no CONCURRENTLY; paste it whole.


---------------------------------------------------------------------------
-- PART 7 — backfill exactly one primary per user.
-- Two statements, never one: solar_systems_one_primary_uk is a plain (non
-- deferrable) partial unique index, so a single UPDATE that flips one row true
-- and another false can transiently violate it depending on row order.
---------------------------------------------------------------------------
update public.solar_systems s
   set is_primary = false
 where s.is_primary
   and s.id <> (select x.id from public.solar_systems x
                 where x.user_id = s.user_id
                 order by (x.solis_station_id is null),
                          x.installation_date asc, x.created_at asc, x.id asc
                 limit 1);

update public.solar_systems s
   set is_primary = true
 where not s.is_primary
   and s.id = (select x.id from public.solar_systems x
                where x.user_id = s.user_id
                order by (x.solis_station_id is null),
                         x.installation_date asc, x.created_at asc, x.id asc
                limit 1)
   and not exists (select 1 from public.solar_systems y
                    where y.user_id = s.user_id and y.is_primary);

---------------------------------------------------------------------------
-- PART 8 — the user_profiles.solis_station_id COMPAT MIRROR.
--
-- It CANNOT be a GENERATED column: a generated column may only reference other
-- columns of its own row, never another table. It must be trigger-maintained.
-- It also cannot be a view, because live crons read the TABLE user_profiles
-- over PostgREST by name.
--
-- The mirror is WRITE-THROUGH, not one-way, because profile-first writes are a
-- real live pattern: onboard_from_station_csv.py:388-396 upserts the profile
-- with a station id BEFORE backfill_history.py creates the solar_systems row.
-- A one-way mirror would silently revert those writes.
--
-- Recursion is prevented by pg_trigger_depth(): a direct write fires at depth 1
-- and the nested write it causes fires at depth 2 and returns immediately. The
-- audit triggers in file 08 have no such guard, so nested changes are still
-- fully logged.
---------------------------------------------------------------------------

create or replace function public.fn_refresh_profile_station(p_user uuid)
returns void
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_station text;
  v_primary uuid;
begin
  if p_user is null then return; end if;

  -- ensure exactly one primary for this user
  if not exists (select 1 from public.solar_systems
                  where user_id = p_user and is_primary) then
    select id into v_primary
      from public.solar_systems
     where user_id = p_user
     order by (solis_station_id is null),
              installation_date asc, created_at asc, id asc
     limit 1;
    if v_primary is not null then
      update public.solar_systems set is_primary = true where id = v_primary;
    end if;
  end if;

  select s.solis_station_id into v_station
    from public.solar_systems s
   where s.user_id = p_user and s.is_primary and s.solis_station_id is not null
   limit 1;

  if v_station is null then
    select s.solis_station_id into v_station
      from public.solar_systems s
     where s.user_id = p_user and s.solis_station_id is not null
     order by s.is_primary desc, s.installation_date asc, s.created_at asc, s.id asc
     limit 1;
  end if;

  -- Never blank the mirror: a NULL here means "no station row yet", which is
  -- the normal mid-onboarding state, and the profile may legitimately hold a
  -- pending id that no station row carries yet.
  if v_station is not null then
    update public.user_profiles p
       set solis_station_id = v_station
     where p.id = p_user
       and p.solis_station_id is distinct from v_station;
  end if;
end $$;

create or replace function public.tg_station_to_profile()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if pg_trigger_depth() > 1 then
    return coalesce(new, old);
  end if;
  if tg_op = 'UPDATE' and old.user_id is distinct from new.user_id then
    perform public.fn_refresh_profile_station(old.user_id);
  end if;
  perform public.fn_refresh_profile_station(coalesce(new.user_id, old.user_id));
  return coalesce(new, old);
end $$;

drop trigger if exists trg_station_to_profile on public.solar_systems;
create trigger trg_station_to_profile
  after insert or delete or update of user_id, solis_station_id, is_primary
  on public.solar_systems
  for each row execute function public.tg_station_to_profile();

create or replace function public.tg_profile_to_station()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_sid text := nullif(btrim(new.solis_station_id), '');
  v_n   integer;
  v_tgt uuid;
begin
  if pg_trigger_depth() > 1 then return new; end if;
  if v_sid is null then return new; end if;
  if tg_op = 'UPDATE' and new.solis_station_id is not distinct from old.solis_station_id then
    return new;
  end if;

  -- (1) the user already owns that station -> promote it. Two statements, for
  --     the same non-deferrable-partial-unique reason as PART 7.
  if exists (select 1 from public.solar_systems
              where user_id = new.id and solis_station_id = v_sid) then
    update public.solar_systems
       set is_primary = false
     where user_id = new.id and is_primary
       and coalesce(solis_station_id, '') <> v_sid;
    update public.solar_systems
       set is_primary = true
     where user_id = new.id and solis_station_id = v_sid and not is_primary;
    return new;
  end if;

  -- (2) the user has station rows but none carries an id yet -> adopt onto the
  --     oldest. This is the onboarding path.
  if not exists (select 1 from public.solar_systems
                  where user_id = new.id and solis_station_id is not null) then
    select id into v_tgt
      from public.solar_systems
     where user_id = new.id and solis_station_id is null
     order by created_at asc, id asc
     limit 1;
    if v_tgt is not null then
      update public.solar_systems set is_primary = false
       where user_id = new.id and is_primary and id <> v_tgt;
      update public.solar_systems
         set solis_station_id = v_sid, is_primary = true
       where id = v_tgt;
      return new;
    end if;
  end if;

  -- (3) otherwise: a pending mapping with no station row yet, or a second
  --     station typed onto the profile. Leave the profile value alone; the
  --     back office creates the station row explicitly.
  return new;
end $$;

drop trigger if exists trg_profile_to_station on public.user_profiles;
create trigger trg_profile_to_station
  after insert or update of solis_station_id
  on public.user_profiles
  for each row execute function public.tg_profile_to_station();

---------------------------------------------------------------------------
-- PART 9 — keep energy_readings.user_id from drifting once the back office can
-- move a station between customers (0 mismatches across all 114,858 rows
-- today; make that structural). system_id becomes authoritative; user_id stays
-- as a denormalised convenience for the portal, not as the authorization key.
-- Cost: one PK lookup into a fully-cached 615-row table per inserted row.
---------------------------------------------------------------------------
create or replace function public.tg_reading_user_matches_system()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare v_owner uuid;
begin
  select user_id into v_owner from public.solar_systems where id = new.system_id;
  if v_owner is not null and new.user_id is distinct from v_owner then
    new.user_id := v_owner;
  end if;
  return new;
end $$;

drop trigger if exists trg_energy_readings_sync_user on public.energy_readings;
create trigger trg_energy_readings_sync_user
  before insert or update of system_id, user_id on public.energy_readings
  for each row execute function public.tg_reading_user_matches_system();

drop trigger if exists trg_energy_readings_5m_sync_user on public.energy_readings_five_minutes;
create trigger trg_energy_readings_5m_sync_user
  before insert or update of system_id, user_id on public.energy_readings_five_minutes
  for each row execute function public.tg_reading_user_matches_system();
