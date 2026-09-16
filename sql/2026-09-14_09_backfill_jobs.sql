-- 2026-09-14 step 09 — the backfill queue (requirement 3).
-- Run this once in the Supabase SQL editor. Order: after 08. Re-runnable.
-- No heavy locks.
--
-- Requirement 3 is "trigger a backfill of daily data for changed records".
-- Firing that inline is how the August duplicate incident happened. Two
-- properties are non-negotiable:
--   * PER-STATION, not per-user. Every existing backfill entry point is
--     user-scoped, including the "skip users who already have readings" guard
--     at backfill_newly_onboarded.py:152-172 — which means a user's SECOND
--     station can never be backfilled at all under today's code.
--   * ENQUEUED, not inline. A 60-day backfill against an API that 502s will
--     exceed any HTTP timeout. The button writes a row; a worker drains it.
--
-- The partial unique index below is the concurrency guard: two engineers
-- editing the same station in the same minute cannot launch two overlapping
-- backfills, and an accidental double-click is a no-op rather than a
-- data-integrity event.

create table if not exists public.backfill_jobs (
  id               uuid primary key default gen_random_uuid(),
  system_id        uuid not null references public.solar_systems(id) on delete cascade,
  solis_station_id text,                -- snapshot: what the id WAS when queued
  granularity      text not null default 'daily'
                     check (granularity in ('daily','five_minutes')),
  date_from        date not null,
  date_to          date not null,
  status           text not null default 'queued'
                     check (status in ('queued','running','succeeded','failed','cancelled')),
  attempt          integer not null default 0,
  rows_written     bigint,
  error            text,
  requested_by     uuid references auth.users(id) on delete set null,
  requested_reason text,
  request_id       uuid,                -- joins to audit_log.request_id
  queued_at        timestamptz not null default now(),
  started_at       timestamptz,
  finished_at      timestamptz,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now(),
  constraint backfill_jobs_date_range_chk check (date_to >= date_from)
);

-- at most one live job per station+granularity
create unique index if not exists backfill_jobs_one_active_uk
  on public.backfill_jobs (system_id, granularity)
  where status in ('queued','running');

create index if not exists backfill_jobs_queue_idx
  on public.backfill_jobs (status, queued_at) where status = 'queued';
create index if not exists backfill_jobs_system_idx
  on public.backfill_jobs (system_id, queued_at desc);

drop trigger if exists trg_backfill_jobs_updated_at on public.backfill_jobs;
create trigger trg_backfill_jobs_updated_at
  before update on public.backfill_jobs
  for each row execute function public.tg_set_updated_at();

alter table public.backfill_jobs enable row level security;
revoke all on public.backfill_jobs from anon, authenticated;

do $$
begin
  if not exists (select 1 from pg_policies
                  where schemaname='public' and tablename='backfill_jobs'
                    and policyname='backfill_jobs_staff_read') then
    create policy backfill_jobs_staff_read on public.backfill_jobs
      for select to authenticated using (public.is_staff());
  end if;
end $$;
grant select on public.backfill_jobs to authenticated;

drop trigger if exists trg_audit_backfill_jobs on public.backfill_jobs;
create trigger trg_audit_backfill_jobs
  after insert or update or delete on public.backfill_jobs
  for each row execute function public.fn_audit_row_changes(
    'status','attempt','rows_written','error','date_from','date_to');

-- Mirror the terminal state onto the station so the grid can show it without a
-- join, and so two engineers can see at a glance which station last succeeded.
create or replace function public.tg_backfill_job_stamp_station()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.status in ('succeeded','failed') then
    update public.solar_systems
       set last_backfill_at     = coalesce(new.finished_at, now()),
           last_backfill_status = new.status
     where id = new.system_id;
  end if;
  return new;
end $$;

drop trigger if exists trg_backfill_job_stamp_station on public.backfill_jobs;
create trigger trg_backfill_job_stamp_station
  after update of status on public.backfill_jobs
  for each row execute function public.tg_backfill_job_stamp_station();
