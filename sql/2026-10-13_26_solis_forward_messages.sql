-- Migration 26 — raw capture of Solis Real-time Data Forwarding messages.
--
-- WHY. Solis pushes every data-logger upload to our MQTT broker (EMQX Cloud,
-- see api/solis_forward.py) instead of us polling stationDay for every plant.
-- During the test (7 collectors agreed with Solis on 2026-10-13) every message
-- is kept verbatim here so it can be compared with the API's curve — point
-- count per day, delay, values, the battery sign — before anything is written
-- to the readings tables. Nothing else reads this table yet; Monitoring
-- Admin's Health tab shows the last hour's counts.
--
-- Volume: ~288 messages per collector per day, a few KB each. The test is
-- ~2k rows/day; a weekly pg_cron job trims rows older than 30 days. Before
-- full opening the ingest moves to the five-minute table and this stays as a
-- short-retention trace.
--
-- RLS like sync_runs (migration 23): staff read via is_staff(), inquiry_ro
-- read, anon nothing. The worker writes over the direct connection (postgres).
--
-- HOW TO RUN: direct Postgres connection, autocommit, one statement at a time
-- (`-- ///` markers). Re-runnable.

create table if not exists public.solis_forward_messages (
  id             bigint generated always as identity primary key,
  received_at    timestamptz not null default now(),
  device_time    timestamptz,            -- message "time" (ms epoch) as sent by Solis
  collector_sn   text,                   -- message "sn"
  is_realtime    boolean,                -- true = live upload, false = historical backfill (isRealtime = 1)
  inverter_count integer,                -- len(inverter1)
  topic          text not null,
  bytes          integer not null,
  payload        jsonb not null          -- the whole message, verbatim
);
-- ///
create index if not exists sfm_collector_time_idx on public.solis_forward_messages (collector_sn, device_time desc);
-- ///
create index if not exists sfm_received_idx on public.solis_forward_messages (received_at desc);
-- ///
alter table public.solis_forward_messages enable row level security;
-- ///
revoke all on public.solis_forward_messages from anon;
-- ///
revoke insert, update, delete on public.solis_forward_messages from authenticated;
-- ///
-- Supabase's default privileges hand `authenticated` ALL on a new public
-- table, and RLS does not govern TRUNCATE: without this line any customer
-- login could empty the table. (Found on apply; migrations 23's sync_runs and
-- station_activity had the same leftover and are closed here too.)
revoke references, trigger, truncate on public.solis_forward_messages, public.sync_runs, public.station_activity from authenticated;
-- ///
grant select on public.solis_forward_messages to authenticated;
-- ///
grant select on public.solis_forward_messages to inquiry_ro;
-- ///
do $$
begin
  if not exists (select 1 from pg_policies where schemaname = 'public' and tablename = 'solis_forward_messages' and policyname = 'sfm_read_staff') then
    create policy sfm_read_staff on public.solis_forward_messages for select to authenticated using (public.is_staff());
  end if;
end $$;
-- ///
do $$
begin
  if not exists (select 1 from cron.job where jobname = 'solis_forward_retention') then
    perform cron.schedule('solis_forward_retention', '41 2 * * 0',
      $cron$delete from public.solis_forward_messages where received_at < now() - interval '30 days'$cron$);
  end if;
end $$;
-- ///
-- Verify
select relname, relrowsecurity from pg_class where relname = 'solis_forward_messages';
-- ///
select policyname, roles from pg_policies where tablename = 'solis_forward_messages';
-- ///
select jobname, schedule from cron.job where jobname = 'solis_forward_retention';

-- ROLLBACK:
--   select cron.unschedule('solis_forward_retention');
--   drop table if exists public.solis_forward_messages;
