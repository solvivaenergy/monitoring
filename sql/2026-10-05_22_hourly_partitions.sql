-- 2026-10-05  22  Hourly readings partitioned by month
--
-- WHY: energy_readings_hourly (file 20) grows ~16k rows a day — ~6 M rows and
-- ~1.5 GB a year at today's fleet, more as it grows. Monthly partitions keep
-- every index small, let a future retention rule ("hourly kept 13 months") be
-- a DROP PARTITION, and make the fleet-wide month re-reads touch one
-- partition instead of the whole table. Same build-and-swap as file 21.
--
-- Design
--   * Range partitions on hour_start, one per calendar month (UTC boundaries;
--     a Manila month starts at 16:00Z on the last day of the previous month,
--     which simply lands in the previous partition — nothing depends on
--     partition = month).
--   * PK becomes (id, hour_start); (system_id, hour_start) stays unique, which
--     is what rollup_hourly() and the worker upsert on.
--   * maintain_hourly_partitions(): pre-creates this month..+2. No drop yet —
--     hourly is kept indefinitely until a retention rule is decided; when it
--     is, add the drop loop here (file 21 shows the pattern). pg_cron daily.
--   * Policies, grants, the updated_at trigger carry over; partitions get no
--     anon/authenticated privileges.
--   * Apply only when no fleet hourly load is running: the copy locks the
--     table for ~30–60 s (~800k rows) and a worker upsert caught under it is
--     retried, but there is no reason to make it.
--
-- Apply over the direct connection (autocommit). Re-runnable.

create or replace function public.maintain_hourly_partitions(p_premake integer default 2)
returns text
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  m    date;
  nm   text;
  made integer := 0;
begin
  for i in 0..p_premake loop
    m  := (date_trunc('month', now() at time zone 'UTC') + (i || ' month')::interval)::date;
    nm := 'energy_readings_hourly_p' || to_char(m, 'YYYY_MM');
    if to_regclass('public.' || nm) is null then
      execute format(
        'create table public.%I partition of public.energy_readings_hourly for values from (%L) to (%L)',
        nm, (m::timestamp at time zone 'UTC'), ((m + interval '1 month')::timestamp at time zone 'UTC'));
      execute format('revoke all on public.%I from anon, authenticated', nm);
      made := made + 1;
    end if;
  end loop;
  return format('%s partition(s) created', made);
end $$;

comment on function public.maintain_hourly_partitions(integer) is
  'Keeps energy_readings_hourly partitioned by UTC month: creates this month..+p_premake. No retention '
  'drop yet (hourly is kept indefinitely). Scheduled daily by pg_cron (job hourly_partitions).';

revoke execute on function public.maintain_hourly_partitions(integer) from public, anon, authenticated;

do $$
declare
  r     record;
  n_old bigint;
  n_new bigint;
begin
  if (select relkind from pg_class where oid = 'public.energy_readings_hourly'::regclass) = 'p' then
    raise notice 'energy_readings_hourly is already partitioned — swap skipped';
    return;
  end if;

  create table public.energy_readings_hourly_new (
    id                uuid not null default gen_random_uuid(),
    system_id         uuid not null,
    user_id           uuid,
    hour_start        timestamptz not null,
    production_kwh    numeric(10,3) not null default 0,
    consumption_kwh   numeric(10,3) not null default 0,
    grid_import_kwh   numeric(10,3) not null default 0,
    grid_export_kwh   numeric(10,3) not null default 0,
    peak_power_kw     numeric(8,3),
    battery_level_end numeric(5,1),
    points            smallint not null default 0,
    source            text not null default 'rollup',
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now(),
    constraint energy_readings_hourly_p_pkey           primary key (id, hour_start),
    constraint energy_readings_hourly_p_system_hour_uk unique (system_id, hour_start),
    constraint energy_readings_hourly_p_system_id_fkey foreign key (system_id) references public.solar_systems(id) on delete cascade,
    constraint energy_readings_hourly_p_user_id_fkey   foreign key (user_id) references auth.users(id) on delete set null,
    constraint energy_readings_hourly_p_points_check   check (points between 0 and 12),
    constraint energy_readings_hourly_p_source_check   check (source in ('rollup','backfill'))
  ) partition by range (hour_start);
  create table public.energy_readings_hourly_new_default partition of public.energy_readings_hourly_new default;

  for r in
    select distinct date_trunc('month', hour_start at time zone 'UTC')::date as m from public.energy_readings_hourly
    union
    select (date_trunc('month', now() at time zone 'UTC') + (g || ' month')::interval)::date from generate_series(0, 2) g
  loop
    execute format(
      'create table public.%I partition of public.energy_readings_hourly_new for values from (%L) to (%L)',
      'energy_readings_hourly_p' || to_char(r.m, 'YYYY_MM'),
      (r.m::timestamp at time zone 'UTC'), ((r.m + interval '1 month')::timestamp at time zone 'UTC'));
  end loop;

  lock table public.energy_readings_hourly in share row exclusive mode;
  insert into public.energy_readings_hourly_new
    (id, system_id, user_id, hour_start, production_kwh, consumption_kwh, grid_import_kwh, grid_export_kwh,
     peak_power_kw, battery_level_end, points, source, created_at, updated_at)
  select id, system_id, user_id, hour_start, production_kwh, consumption_kwh, grid_import_kwh, grid_export_kwh,
         peak_power_kw, battery_level_end, points, source, created_at, updated_at
    from public.energy_readings_hourly;
  select count(*) into n_old from public.energy_readings_hourly;
  select count(*) into n_new from public.energy_readings_hourly_new;
  if n_old <> n_new then
    raise exception 'copy mismatch: % rows in the old table, % in the new', n_old, n_new;
  end if;

  create trigger trg_energy_readings_hourly_updated_at
    before update on public.energy_readings_hourly_new
    for each row execute function public.tg_set_updated_at();

  alter table public.energy_readings_hourly     rename to energy_readings_hourly_old;
  alter table public.energy_readings_hourly_new rename to energy_readings_hourly;
  alter table public.energy_readings_hourly_new_default rename to energy_readings_hourly_default;
  raise notice 'swapped: % row(s) copied into the partitioned table', n_new;
end $$;

comment on table public.energy_readings_hourly is
  'One row per station per Manila hour (file 20), partitioned by UTC month (file 22). Rolled up from '
  'energy_readings_five_minutes by rollup_hourly() or rebuilt from Solis stationDay by the backfill worker '
  '(granularity ''hourly''). points = five-minute samples in the hour (12 = complete). Today''s hours are '
  'NOT here while the day is running: /app/hourly computes them live from the five-minute table.';

alter table public.energy_readings_hourly enable row level security;
revoke all on public.energy_readings_hourly from anon;
revoke insert, update, delete on public.energy_readings_hourly from authenticated;
grant select on public.energy_readings_hourly to authenticated;
grant select on public.energy_readings_hourly to inquiry_ro;

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

do $$
declare r record;
begin
  for r in select c.relname from pg_inherits i join pg_class c on c.oid = i.inhrelid
            where i.inhparent = 'public.energy_readings_hourly'::regclass loop
    execute format('revoke all on public.%I from anon, authenticated', r.relname);
  end loop;
end $$;

select cron.schedule('hourly_partitions', '17 1 * * *', $cron$select public.maintain_hourly_partitions()$cron$);

select pg_notify('pgrst', 'reload schema');

-- Verify
select c.relname, pg_get_expr(c.relpartbound, c.oid) as bound, pg_size_pretty(pg_total_relation_size(c.oid)) as size
  from pg_inherits i join pg_class c on c.oid = i.inhrelid
 where i.inhparent = 'public.energy_readings_hourly'::regclass order by 1;
select jobname, schedule from cron.job where jobname = 'hourly_partitions';

-- After a day with roll-ups landing on the new table:
--   drop table public.energy_readings_hourly_old;
