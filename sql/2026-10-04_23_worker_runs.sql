-- 2026-10-04  23  Worker bookkeeping: sync_runs (every pipeline run) and station_activity (what the portal is looking at)
--
-- WHY: the pipelines move into one always-on worker (api/worker.py). Two things
-- it needs that four crons never had:
--   * sync_runs — one row per run of each task (five_minute, nightly, monthly,
--     health), with status, stats and the error text. Monitoring Admin's
--     Health tab reads it for "when did each feed last succeed", and the
--     September outage (feed dead ~20 h, noticed by a person) becomes an alert
--     within 30 minutes instead.
--   * station_activity — last_viewed_at per station, touched by /app/live and
--     /app/hourly. The worker polls Solis every 5 minutes only for stations
--     someone looked at in the last hour and every 15 minutes for the rest
--     ("cadence by demand", ~3× fewer Solis calls at today's usage).
-- Both are service-role tables: customers never read them; staff read them
-- through Monitoring Admin. sync_runs is trimmed to 90 days weekly by pg_cron.
--
-- Apply over the direct connection (autocommit). Re-runnable.

create table if not exists public.sync_runs (
  id          bigserial primary key,
  task        text not null,
  started_at  timestamptz not null default now(),
  finished_at timestamptz,
  status      text not null default 'running' check (status in ('running','succeeded','failed')),
  stats       jsonb not null default '{}'::jsonb,
  error       text,
  host        text
);
create index if not exists sync_runs_task_started_idx on public.sync_runs (task, started_at desc);
comment on table public.sync_runs is
  'One row per run of a worker task (five_minute, nightly, monthly, health, …): when, how long, outcome, '
  'stats. Written by api/worker.py; read by Monitoring Admin → Health. Trimmed to 90 days weekly.';

alter table public.sync_runs enable row level security;
revoke all on public.sync_runs from anon;
revoke insert, update, delete on public.sync_runs from authenticated;
grant select on public.sync_runs to authenticated;
grant select on public.sync_runs to inquiry_ro;
do $$
begin
  if not exists (select 1 from pg_policies where schemaname='public' and tablename='sync_runs' and policyname='sr_read_staff') then
    create policy sr_read_staff on public.sync_runs for select to authenticated using (public.is_staff());
  end if;
end $$;

create table if not exists public.station_activity (
  system_id      uuid primary key references public.solar_systems(id) on delete cascade,
  last_viewed_at timestamptz not null default now(),
  views          bigint not null default 0
);
comment on table public.station_activity is
  'Last time a portal login looked at this station (/app/live, /app/hourly). The worker polls Solis every '
  '5 minutes for stations viewed in the last hour and every 15 minutes for the rest.';

alter table public.station_activity enable row level security;
revoke all on public.station_activity from anon;
revoke insert, update, delete on public.station_activity from authenticated;
grant select on public.station_activity to authenticated;
grant select on public.station_activity to inquiry_ro;
do $$
begin
  if not exists (select 1 from pg_policies where schemaname='public' and tablename='station_activity' and policyname='sa2_read_staff') then
    create policy sa2_read_staff on public.station_activity for select to authenticated using (public.is_staff());
  end if;
end $$;

-- Called by the API (service role) with the Solis station id it already has.
create or replace function public.touch_station_activity(p_station_id text)
returns void
language sql
security definer
set search_path = public, pg_temp
as $$
  insert into public.station_activity (system_id, last_viewed_at, views)
  select s.id, now(), 1 from public.solar_systems s where s.solis_station_id = p_station_id
  on conflict (system_id) do update set last_viewed_at = now(), views = public.station_activity.views + 1;
$$;
revoke execute on function public.touch_station_activity(text) from public, anon, authenticated;
grant  execute on function public.touch_station_activity(text) to service_role;

select cron.schedule('sync_runs_retention', '23 2 * * 0',
                     $cron$delete from public.sync_runs where started_at < now() - interval '90 days'$cron$);

select pg_notify('pgrst', 'reload schema');

-- Verify
select tablename, policyname from pg_policies where tablename in ('sync_runs','station_activity') order by 1, 2;
select jobname, schedule from cron.job where jobname = 'sync_runs_retention';
