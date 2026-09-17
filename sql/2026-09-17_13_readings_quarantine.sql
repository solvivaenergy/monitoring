-- 2026-09-17 step 13 — quarantine for readings captured under the WRONG station.
-- Run over the direct connection (autocommit). Re-runnable. No heavy locks.
--
-- WHY: when a customer's Solis station id was wrong on the Odoo lead, every
-- nightly sync since onboarding stored ANOTHER plant's production under this
-- customer — 396 days in the worst case found on 2026-09-17. Correcting the id
-- is not enough: the backfill of the correct station only overwrites the dates
-- the correct station has data for; every other day would keep the wrong
-- plant's number, silently. The remap operation therefore moves ALL readings
-- captured under the old id here, in the same transaction as the id change,
-- then enqueues a backfill of the correct station from its first-power date.
--
-- Quarantine, not delete: the rows are recoverable (a remap done in error can
-- be reversed), auditable (who, why, from which id to which), and reportable
-- (how long was the customer looking at someone else's numbers). Same
-- philosophy as file 07's ON DELETE RESTRICT and file 12's legacy quarantine.
--
-- Column order matters: LIKE copies energy_readings' columns first, in its
-- order, then the extras below in this order. api/monitoring_admin_routes.py relies
-- on that with `insert into ... select r.*, now(), %s, ...`.

create table if not exists public.energy_readings_quarantine (
  like public.energy_readings including defaults,
  quarantined_at   timestamptz not null default now(),
  quarantined_by   uuid references auth.users(id) on delete set null,
  request_id       uuid,                 -- joins to audit_log.request_id and backfill_jobs.request_id
  reason           text,
  from_station_id  text,                 -- the station the rows were captured under
  to_station_id    text                  -- the station the system was remapped to
);

do $$
begin
  if not exists (select 1 from pg_constraint
                  where conrelid = 'public.energy_readings_quarantine'::regclass
                    and contype = 'p') then
    alter table public.energy_readings_quarantine add primary key (id);
  end if;
end $$;

create index if not exists energy_readings_quarantine_system_idx
  on public.energy_readings_quarantine (system_id, quarantined_at desc);
create index if not exists energy_readings_quarantine_request_idx
  on public.energy_readings_quarantine (request_id);

alter table public.energy_readings_quarantine enable row level security;
revoke all on public.energy_readings_quarantine from anon, authenticated;

do $$
begin
  if not exists (select 1 from pg_policies
                  where schemaname = 'public' and tablename = 'energy_readings_quarantine'
                    and policyname = 'erq_staff_read') then
    create policy erq_staff_read on public.energy_readings_quarantine
      for select to authenticated using (public.is_staff());
  end if;
end $$;
grant select on public.energy_readings_quarantine to authenticated;

-- Verify
select count(*) as quarantined_rows from public.energy_readings_quarantine;
