-- 2026-10-03  20  Hourly readings: a roll-up of the five-minute cache, plus a Solis backfill path
--
-- WHY: the portal mockup shows hourly energy, and we hold only two shapes —
-- the five-minute table, a ROLLING ONE-DAY cache purged at Manila midnight,
-- and one row per day. Hourly for any past day was therefore impossible.
-- Rather than a third Solis feed, this file adds one table that is FILLED
-- FROM THE FIVE-MINUTE ROWS WE ALREADY FETCH (an hour is the sum of its 12
-- slices) and can be rebuilt from Solis's stationDay curve for history.
--
-- Design points (decided with Alden on 2026-10-03, "option A"):
--   * Today's hours are served LIVE from the five-minute table by /app/hourly;
--     nothing writes partial hours here every five minutes (that would be
--     ~180k row writes a day on the NANO instance — the write-amplification
--     pattern migration 17 removed). The five-minute cron calls
--     rollup_hourly() once per run for the two hours just closed (~1,300 rows
--     evaluated, almost all unchanged → no write), for yesterday right before
--     the midnight purge, and for the whole day in its last runs.
--   * `points` = five-minute samples in the hour (12 = complete). The roll-up
--     never replaces a fuller hour with a thinner one, and an identical row is
--     not rewritten (the WHERE on the upsert), so re-running it is free.
--   * `source` says whether the row came from the live roll-up or a Solis
--     backfill (the worker's new 'hourly' granularity, one stationDay call per
--     station-day — ~90 days × 680 stations for the initial history).
--   * Read policies mirror the daily table: owner OR view-grantee through
--     owns_system(), staff read all, service_role writes, anon nothing.
--   * Kept indefinitely: ~6 M rows / ~0.6 GB a year at today's fleet size.
--
-- Apply over the direct connection with autocommit (see sql/APPLIED.md). Re-runnable.

create table if not exists public.energy_readings_hourly (
  id                 uuid primary key default gen_random_uuid(),
  system_id          uuid not null references public.solar_systems(id) on delete cascade,
  user_id            uuid references auth.users(id) on delete set null,
  hour_start         timestamptz not null,
  production_kwh     numeric(10,3) not null default 0,
  consumption_kwh    numeric(10,3) not null default 0,
  grid_import_kwh    numeric(10,3) not null default 0,
  grid_export_kwh    numeric(10,3) not null default 0,
  peak_power_kw      numeric(8,3),
  battery_level_end  numeric(5,1),
  points             smallint not null default 0 check (points between 0 and 12),
  source             text not null default 'rollup' check (source in ('rollup','backfill')),
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now(),
  constraint energy_readings_hourly_system_hour_uk unique (system_id, hour_start)
);

comment on table public.energy_readings_hourly is
  'One row per station per Manila hour. Rolled up from energy_readings_five_minutes by rollup_hourly() '
  '(called by the five-minute cron) or rebuilt from Solis stationDay by the backfill worker (granularity '
  '''hourly''). points = five-minute samples in the hour (12 = complete). Today''s hours are NOT here while '
  'the day is running: /app/hourly computes them live from the five-minute table.';

drop trigger if exists trg_energy_readings_hourly_updated_at on public.energy_readings_hourly;
create trigger trg_energy_readings_hourly_updated_at
  before update on public.energy_readings_hourly
  for each row execute function public.tg_set_updated_at();

-- Locked down like the other reading tables (file 10 pattern).
alter table public.energy_readings_hourly enable row level security;
revoke all on public.energy_readings_hourly from anon;
revoke insert, update, delete on public.energy_readings_hourly from authenticated;
grant select on public.energy_readings_hourly to authenticated;

do $$
begin
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='energy_readings_hourly' and policyname='erh_read_own_station') then
    create policy erh_read_own_station on public.energy_readings_hourly
      for select to authenticated using (public.owns_system(system_id));
  end if;
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='energy_readings_hourly' and policyname='erh_read_staff') then
    create policy erh_read_staff on public.energy_readings_hourly
      for select to authenticated using (public.is_staff());
  end if;
end $$;

-- The roll-up. Hour boundaries are taken in Asia/Manila explicitly, so the
-- result does not depend on the session's TimeZone setting (Manila is a
-- whole-hour offset, so the boundaries coincide with UTC hours anyway).
create or replace function public.rollup_hourly(p_from timestamptz, p_to timestamptz)
returns integer
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  n integer;
begin
  insert into public.energy_readings_hourly
    (system_id, user_id, hour_start, production_kwh, consumption_kwh,
     grid_import_kwh, grid_export_kwh, peak_power_kw, battery_level_end, points, source)
  select f.system_id,
         (array_agg(f.user_id) filter (where f.user_id is not null))[1],   -- no max(uuid) in Postgres
         (date_trunc('hour', f."timestamp" at time zone 'Asia/Manila') at time zone 'Asia/Manila') as hour_start,
         round(sum(f.production_kwh)::numeric, 3),
         round(sum(f.consumption_kwh)::numeric, 3),
         round(sum(f.grid_import_kwh)::numeric, 3),
         round(sum(f.grid_export_kwh)::numeric, 3),
         round((max(f.production_kwh) * 12)::numeric, 3),              -- the biggest 5-min slice, as kW
         (array_agg(f.battery_level order by f."timestamp" desc) filter (where f.battery_level is not null))[1],
         least(count(*), 12)::smallint,
         'rollup'
    from public.energy_readings_five_minutes f
   where f."timestamp" >= p_from and f."timestamp" < p_to
     and f.system_id is not null
   group by f.system_id, 3
  on conflict (system_id, hour_start) do update
     set production_kwh    = excluded.production_kwh,
         consumption_kwh   = excluded.consumption_kwh,
         grid_import_kwh   = excluded.grid_import_kwh,
         grid_export_kwh   = excluded.grid_export_kwh,
         peak_power_kw     = excluded.peak_power_kw,
         battery_level_end = excluded.battery_level_end,
         points            = excluded.points,
         source            = 'rollup',
         user_id           = coalesce(excluded.user_id, public.energy_readings_hourly.user_id)
   where excluded.points >= public.energy_readings_hourly.points
     and (public.energy_readings_hourly.production_kwh, public.energy_readings_hourly.consumption_kwh,
          public.energy_readings_hourly.grid_import_kwh, public.energy_readings_hourly.grid_export_kwh,
          public.energy_readings_hourly.points, public.energy_readings_hourly.battery_level_end)
         is distinct from
         (excluded.production_kwh, excluded.consumption_kwh,
          excluded.grid_import_kwh, excluded.grid_export_kwh,
          excluded.points, excluded.battery_level_end);
  get diagnostics n = row_count;
  return n;
end $$;

comment on function public.rollup_hourly(timestamptz, timestamptz) is
  'Aggregate energy_readings_five_minutes into energy_readings_hourly for [p_from, p_to). Idempotent: '
  'never replaces a fuller hour with a thinner one, never rewrites an identical row. Returns rows written.';

revoke execute on function public.rollup_hourly(timestamptz, timestamptz) from public, anon, authenticated;
grant  execute on function public.rollup_hourly(timestamptz, timestamptz) to service_role;

-- The worker's new granularity.
alter table public.backfill_jobs drop constraint if exists backfill_jobs_granularity_check;
alter table public.backfill_jobs
  add constraint backfill_jobs_granularity_check check (granularity in ('daily','five_minutes','hourly'));

-- Verify
select tablename, policyname, cmd from pg_policies
 where schemaname = 'public' and tablename = 'energy_readings_hourly' order by 2;
select pg_get_constraintdef(oid) from pg_constraint where conname = 'backfill_jobs_granularity_check';

-- Rollback, if ever needed:
--   drop function if exists public.rollup_hourly(timestamptz, timestamptz);
--   drop table if exists public.energy_readings_hourly;
--   alter table public.backfill_jobs drop constraint backfill_jobs_granularity_check;
--   alter table public.backfill_jobs add constraint backfill_jobs_granularity_check
--     check (granularity in ('daily','five_minutes'));
