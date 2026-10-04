-- 2026-10-04  21  Five-minute cache partitioned by UTC day; retention is DROP PARTITION, not DELETE
--
-- WHY: energy_readings_five_minutes is a rolling cache (~130k rows, 46 MB a
-- day) whose retention was a DELETE at Manila midnight. That DELETE killed the
-- feed for ~20 hours on 2026-09-15 (PostgREST's 8 s statement timeout), and
-- even bounded in hour-windows it churns WAL and dirties pages every night
-- (the Disk IO Budget alerts of September). Dropping a partition is instant,
-- writes almost nothing, and cannot time out.
--
-- Design
--   * Range partitions on "timestamp", one per UTC day. Manila is UTC+8, so a
--     Manila day spans two UTC partitions; retention keeps the 2 UTC days
--     before today, which always covers "yesterday Manila" — the day the
--     hourly roll-up reads right before the old purge would have run.
--   * maintain_five_minute_partitions(): pre-creates today..today+2, drops
--     partitions older than p_keep_days, empties stragglers in the default
--     partition. pg_cron runs it hourly (job 'five_minute_partitions').
--   * The PK becomes (id, "timestamp"): a partitioned table's unique keys
--     must include the partition column. Nothing references the id.
--   * fillfactor 85 (file 17) is set per partition: a partitioned parent
--     cannot carry storage parameters.
--   * Partitions are tables in public, so PostgREST exposes them; anon and
--     authenticated get no privileges on them. Clients use the parent, whose
--     RLS policy (owns_system) and grants are the same as before.
--   * Build-and-swap in ONE transaction (the DO block): Postgres cannot
--     convert a table in place. The lock blocks the five-minute writer for the
--     few seconds of the copy; PostgREST waits up to 8 s, and the sync retries.
--     The old table stays as energy_readings_five_minutes_old for a day, then
--     drop it by hand (see the end of this file).
--   * The four Python pipelines are unchanged except that the purge call in
--     api/sync_five_minutes_to_supabase.py is gone.
--
-- Apply over the direct connection (autocommit). Re-runnable: the swap is
-- skipped once the table is partitioned; everything else is idempotent.

create extension if not exists pg_cron;

---------------------------------------------------------------------------
-- Partition maintenance, run hourly by pg_cron
---------------------------------------------------------------------------
create or replace function public.maintain_five_minute_partitions(p_keep_days integer default 2,
                                                                  p_premake integer default 2)
returns text
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  d       date;
  nm      text;
  made    integer := 0;
  dropped integer := 0;
  cutoff  date;
  r       record;
begin
  -- today (UTC) and the next p_premake days always exist
  for i in 0..p_premake loop
    d  := (now() at time zone 'UTC')::date + i;
    nm := 'energy_readings_five_minutes_p' || to_char(d, 'YYYY_MM_DD');
    if to_regclass('public.' || nm) is null then
      execute format(
        'create table public.%I partition of public.energy_readings_five_minutes '
        'for values from (%L) to (%L) with (fillfactor = 85)',
        nm, (d::timestamp at time zone 'UTC'), ((d + 1)::timestamp at time zone 'UTC'));
      execute format('revoke all on public.%I from anon, authenticated', nm);
      made := made + 1;
    end if;
  end loop;

  -- partitions whose day is older than the retention are dropped whole
  cutoff := (now() at time zone 'UTC')::date - p_keep_days;
  for r in
    select c.relname
      from pg_inherits i join pg_class c on c.oid = i.inhrelid
     where i.inhparent = 'public.energy_readings_five_minutes'::regclass
       and c.relname ~ '^energy_readings_five_minutes_p\d{4}_\d{2}_\d{2}$'
       and to_date(right(c.relname, 10), 'YYYY_MM_DD') < cutoff
  loop
    execute format('drop table public.%I', r.relname);
    dropped := dropped + 1;
  end loop;

  -- a row that missed every partition lands in the default one; keep it tidy
  if to_regclass('public.energy_readings_five_minutes_default') is not null then
    execute format('delete from public.energy_readings_five_minutes_default where "timestamp" < %L',
                   (cutoff::timestamp at time zone 'UTC'));
  end if;

  return format('%s partition(s) created, %s dropped', made, dropped);
end $$;

comment on function public.maintain_five_minute_partitions(integer, integer) is
  'Keeps energy_readings_five_minutes partitioned by UTC day: creates today..today+p_premake, drops '
  'partitions older than p_keep_days. Scheduled hourly by pg_cron (job five_minute_partitions).';

revoke execute on function public.maintain_five_minute_partitions(integer, integer) from public, anon, authenticated;

---------------------------------------------------------------------------
-- Build the partitioned table and swap it in — one transaction, once
---------------------------------------------------------------------------
do $$
declare
  r     record;
  n_old bigint;
  n_new bigint;
begin
  if (select relkind from pg_class where oid = 'public.energy_readings_five_minutes'::regclass) = 'p' then
    raise notice 'energy_readings_five_minutes is already partitioned — swap skipped';
    return;
  end if;

  create table public.energy_readings_five_minutes_new (
    id               uuid not null default extensions.uuid_generate_v4(),
    user_id          uuid not null,
    system_id        uuid not null,
    "timestamp"      timestamptz not null,
    production_kwh   numeric not null,
    consumption_kwh  numeric not null,
    battery_level    numeric,
    battery_status   text,
    grid_import_kwh  numeric default 0,
    grid_export_kwh  numeric default 0,
    created_at       timestamptz default now(),
    daily_earning    numeric default 0,
    lifetime_earning numeric default 0,
    constraint energy_readings_5m_p_pkey                 primary key (id, "timestamp"),
    constraint energy_readings_5m_p_system_ts_uk         unique (system_id, "timestamp"),
    constraint energy_readings_5m_p_system_id_fkey       foreign key (system_id) references public.solar_systems(id),
    constraint energy_readings_5m_p_user_id_fkey         foreign key (user_id) references auth.users(id),
    constraint energy_readings_5m_p_user_profile_fkey    foreign key (user_id) references public.user_profiles(id) on delete restrict,
    constraint energy_readings_5m_p_battery_status_check check (battery_status = any (array['charging','discharging','idle','full']))
  ) partition by range ("timestamp");
  create index energy_readings_5m_p_timestamp_idx on public.energy_readings_five_minutes_new ("timestamp");
  create table public.energy_readings_five_minutes_new_default
    partition of public.energy_readings_five_minutes_new default;

  -- one partition per UTC day already present, plus today..today+2
  for r in
    select distinct ("timestamp" at time zone 'UTC')::date as d from public.energy_readings_five_minutes
    union
    select (now() at time zone 'UTC')::date + g from generate_series(0, 2) g
  loop
    execute format(
      'create table public.%I partition of public.energy_readings_five_minutes_new '
      'for values from (%L) to (%L) with (fillfactor = 85)',
      'energy_readings_five_minutes_p' || to_char(r.d, 'YYYY_MM_DD'),
      (r.d::timestamp at time zone 'UTC'), ((r.d + 1)::timestamp at time zone 'UTC'));
  end loop;

  -- copy under a lock that holds the five-minute writer off for a few seconds
  lock table public.energy_readings_five_minutes in share row exclusive mode;
  insert into public.energy_readings_five_minutes_new select * from public.energy_readings_five_minutes;
  select count(*) into n_old from public.energy_readings_five_minutes;
  select count(*) into n_new from public.energy_readings_five_minutes_new;
  if n_old <> n_new then
    raise exception 'copy mismatch: % rows in the old table, % in the new', n_old, n_new;
  end if;

  -- the 04c owner check, after the copy (the copied rows already passed it)
  create trigger trg_energy_readings_5m_sync_user
    before insert or update of system_id, user_id on public.energy_readings_five_minutes_new
    for each row execute function public.tg_reading_user_matches_system();

  alter table public.energy_readings_five_minutes     rename to energy_readings_five_minutes_old;
  alter table public.energy_readings_five_minutes_new rename to energy_readings_five_minutes;
  alter table public.energy_readings_five_minutes_new_default rename to energy_readings_five_minutes_default;
  raise notice 'swapped: % row(s) copied into the partitioned table', n_new;
end $$;

---------------------------------------------------------------------------
-- Access (file 10 pattern), the watermark view (file 17), the cron job
---------------------------------------------------------------------------
alter table public.energy_readings_five_minutes enable row level security;
revoke all on public.energy_readings_five_minutes from anon;
revoke insert, update, delete on public.energy_readings_five_minutes from authenticated;
grant select on public.energy_readings_five_minutes to authenticated;
grant select on public.energy_readings_five_minutes to inquiry_ro;

do $$
begin
  if not exists (select 1 from pg_policies where schemaname = 'public'
                  and tablename = 'energy_readings_five_minutes' and policyname = 'er5m_read_own_station') then
    create policy er5m_read_own_station on public.energy_readings_five_minutes
      for select to authenticated using (public.owns_system(system_id));
  end if;
end $$;

-- partitions: the parent is the API surface; nobody reads a partition directly
do $$
declare r record;
begin
  for r in select c.relname from pg_inherits i join pg_class c on c.oid = i.inhrelid
            where i.inhparent = 'public.energy_readings_five_minutes'::regclass loop
    execute format('revoke all on public.%I from anon, authenticated', r.relname);
  end loop;
end $$;

-- The view followed the OLD table through the rename (views bind by OID);
-- re-creating it by name binds it to the partitioned table.
create or replace view public.five_minute_watermarks with (security_invoker = on) as
with today as (
  select system_id, "timestamp", lifetime_earning
    from public.energy_readings_five_minutes
   where "timestamp" >= (date_trunc('day', now() at time zone 'Asia/Manila') at time zone 'Asia/Manila')
)
select l.system_id, l."timestamp" as last_ts, l.lifetime_earning as last_lifetime_earning, c.n as n_rows
  from (select distinct on (system_id) system_id, "timestamp", lifetime_earning
          from today order by system_id, "timestamp" desc) l
  join (select system_id, count(*) as n from today group by system_id) c using (system_id);
revoke all on public.five_minute_watermarks from anon, authenticated;
grant select on public.five_minute_watermarks to inquiry_ro;

select cron.schedule('five_minute_partitions', '7 * * * *', $cron$select public.maintain_five_minute_partitions()$cron$);

select pg_notify('pgrst', 'reload schema');

-- Verify
select c.relname, c.relkind, pg_get_expr(c.relpartbound, c.oid) as bound,
       pg_size_pretty(pg_total_relation_size(c.oid)) as size
  from pg_inherits i join pg_class c on c.oid = i.inhrelid
 where i.inhparent = 'public.energy_readings_five_minutes'::regclass order by 1;
select jobname, schedule, command from cron.job where jobname = 'five_minute_partitions';

-- After a day with the feed healthy on the new table:
--   drop table public.energy_readings_five_minutes_old;
-- Rollback before that point (one transaction):
--   alter table public.energy_readings_five_minutes rename to energy_readings_five_minutes_p;
--   alter table public.energy_readings_five_minutes_old rename to energy_readings_five_minutes;
--   then re-create five_minute_watermarks as above and notify pgrst.
