-- 2026-09-14 step 08 — staff identity and the audit trail.
-- Run this once in the Supabase SQL editor. Order: after 07. Re-runnable.
-- No heavy locks (new tables; the triggers attach to existing ones with a
-- brief ACCESS EXCLUSIVE that is catalog-only).
--
-- WHY A TABLE AND NOT A FLAG OR A CLAIM:
--   * There is NO admin/staff concept anywhere in this project today. All 620
--     auth.users have role='authenticated' and identical app_metadata
--     {provider, providers} — zero custom claims in use. The engineering team
--     has no accounts here at all, only test@ and demo@solvivaenergy.com.
--     Everything staff-facing runs on the service-role key. There is nothing to
--     extend; this is from zero.
--   * user_metadata is DISQUALIFIED OUTRIGHT: it is writable by the user
--     themselves via PUT /auth/v1/user, which was demonstrated end to end
--     against a throwaway account. A user_metadata.role='staff' check would be
--     a self-service admin button.
--   * app_metadata is service-role-write-only and safe, but a claim is baked
--     into the token at issue time, so revoking access does not take effect
--     until the token expires. For ~10 internal users, a table is queryable,
--     revocable instantly, joinable to the audit trail, and greppable.

---------------------------------------------------------------------------
-- staff_users
---------------------------------------------------------------------------
create table if not exists public.staff_users (
  user_id     uuid primary key references auth.users(id) on delete cascade,
  email       text not null,
  role        text not null default 'readonly'
                check (role in ('readonly','engineer','admin')),
  active      boolean not null default true,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  created_by  uuid references auth.users(id),
  revoked_at  timestamptz
);

-- Default 'readonly' on purpose: the list view is 90% of the back office's
-- value and carries none of the risk. Promote to 'engineer' deliberately.

create index if not exists staff_users_active_idx
  on public.staff_users (active) where active and revoked_at is null;

drop trigger if exists trg_staff_users_updated_at on public.staff_users;
create trigger trg_staff_users_updated_at
  before update on public.staff_users
  for each row execute function public.tg_set_updated_at();

alter table public.staff_users enable row level security;
revoke all on public.staff_users from anon, authenticated;
-- No policies at all: anon and authenticated see nothing; service_role bypasses
-- RLS entirely. The grants matter INDEPENDENTLY of RLS — PostgREST checks the
-- grant first, and a table with RLS but no policy returns a confusing empty set
-- instead of a clean denial.

---------------------------------------------------------------------------
-- is_staff() — the canonical predicate for any future RLS policy.
-- SECURITY DEFINER so a policy on another table can consult staff_users without
-- the caller holding SELECT on it. `set search_path` is MANDATORY on a SECURITY
-- DEFINER function: without it, a caller who can create objects can shadow
-- staff_users and make this return true.
---------------------------------------------------------------------------
create or replace function public.is_staff()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select exists (
    select 1 from public.staff_users
     where user_id = auth.uid() and active and revoked_at is null
  );
$$;
revoke execute on function public.is_staff() from public, anon;
grant  execute on function public.is_staff() to authenticated;

create or replace function public.staff_role()
returns text
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select role from public.staff_users
   where user_id = auth.uid() and active and revoked_at is null;
$$;
revoke execute on function public.staff_role() from public, anon;
grant  execute on function public.staff_role() to authenticated;

---------------------------------------------------------------------------
-- audit_log
--
-- DESIGN CONSTRAINT that decides trigger-vs-application: the back office writes
-- through a service holding the SERVICE-ROLE key, and auth.uid() is NULL under
-- service_role. A pure trigger therefore cannot see who acted. But
-- application-level auditing alone misses the eleven other scripts in api/ that
-- write with the same key and can silently change a mapping.
-- => Use BOTH. The trigger is the backstop (same transaction as the write, so
--    it cannot desynchronise); the application supplies identity through
--    transaction-local GUCs. A write from a script that was never updated
--    records actor_kind='unknown', which is itself the signal you want.
---------------------------------------------------------------------------
create table if not exists public.audit_log (
  id           bigserial primary key,
  occurred_at  timestamptz not null default now(),
  actor_id     uuid references auth.users(id) on delete set null,
  actor_email  text,                    -- denormalised: survives account deletion
  actor_kind   text not null default 'unknown'
                 check (actor_kind in ('staff','job','mcp','unknown')),
  table_name   text not null,
  row_pk       text not null,           -- text: PKs here are uuid and bigint
  operation    text not null check (operation in ('INSERT','UPDATE','DELETE')),
  field_name   text,                    -- null for whole-row INSERT/DELETE
  old_value    text,
  new_value    text,
  reason       text,                    -- typed by the operator, see below
  request_id   uuid,                    -- ties a multi-field edit together
  source       text                     -- 'backoffice' | 'backfill_history.py' | ...
);

create index if not exists audit_log_row_idx
  on public.audit_log (table_name, row_pk, occurred_at desc);
create index if not exists audit_log_actor_idx
  on public.audit_log (actor_id, occurred_at desc);
create index if not exists audit_log_occurred_idx
  on public.audit_log (occurred_at desc);

alter table public.audit_log enable row level security;
revoke all on public.audit_log from anon, authenticated;

do $$
begin
  if not exists (select 1 from pg_policies
                  where schemaname = 'public' and tablename = 'audit_log'
                    and policyname = 'audit_log_staff_read') then
    create policy audit_log_staff_read on public.audit_log
      for select to authenticated using (public.is_staff());
  end if;
end $$;
grant select on public.audit_log to authenticated;

-- Append-only. ORIGINALLY two rewrite rules (do instead nothing on update /
-- delete). REPLACED 2026-09-17 by migration 14: the rules rewrote the FK's
-- internal "UPDATE audit_log SET actor_id = NULL" (ON DELETE SET NULL from
-- auth.users) into nothing, and Postgres's RI check then failed — which made
-- EVERY auth user in the project undeletable. The trigger below refuses all
-- deletes and every update except that one FK write. Kept here so this file
-- reflects production; file 14 is the record of the change.
create or replace function public.tg_audit_log_append_only()
returns trigger
language plpgsql
as $$
begin
  if tg_op = 'DELETE' then
    raise exception 'audit_log is append-only (delete refused)';
  end if;
  if old.actor_id is not null and new.actor_id is null
     and row(new.id, new.occurred_at, new.actor_email, new.actor_kind, new.table_name, new.row_pk,
             new.operation, new.field_name, new.old_value, new.new_value, new.reason, new.request_id, new.source)
         is not distinct from
         row(old.id, old.occurred_at, old.actor_email, old.actor_kind, old.table_name, old.row_pk,
             old.operation, old.field_name, old.old_value, old.new_value, old.reason, old.request_id, old.source)
  then
    return new;
  end if;
  raise exception 'audit_log is append-only (update refused)';
end $$;

drop trigger if exists trg_audit_log_append_only on public.audit_log;
create trigger trg_audit_log_append_only
  before update or delete on public.audit_log
  for each row execute function public.tg_audit_log_append_only();

---------------------------------------------------------------------------
-- The audit trigger. Column names come from tg_argv; values are read out of
-- to_jsonb(NEW/OLD) rather than dynamic SQL, which is both faster and immune to
-- identifier-quoting mistakes.
---------------------------------------------------------------------------
create or replace function public.fn_audit_row_changes()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  col     text;
  old_j   jsonb := case when tg_op <> 'INSERT' then to_jsonb(old) end;
  new_j   jsonb := case when tg_op <> 'DELETE' then to_jsonb(new) end;
  old_v   text;
  new_v   text;
  a_id    uuid := nullif(current_setting('app.actor_id',    true), '')::uuid;
  a_mail  text := nullif(current_setting('app.actor_email', true), '');
  a_rsn   text := nullif(current_setting('app.reason',      true), '');
  a_req   uuid := nullif(current_setting('app.request_id',  true), '')::uuid;
  a_src   text := coalesce(nullif(current_setting('app.source', true), ''), 'unknown');
  a_kind  text;
  -- Not every audited table keys on "id": staff_users keys on user_id. As
  -- first written this was coalesce(new_j->>'id', old_j->>'id') and returned
  -- NULL for staff_users, so the INSERT of the very first staff row failed on
  -- row_pk's NOT NULL (2026-09-17). Fall through the known key columns and
  -- never return NULL. Replaced in production the same day.
  pk      text := coalesce(new_j ->> 'id', old_j ->> 'id',
                           new_j ->> 'user_id', old_j ->> 'user_id', '(no pk)');
begin
  a_id   := coalesce(a_id, auth.uid());
  a_kind := case when a_id is not null      then 'staff'
                 when a_src <> 'unknown'    then 'job'
                 else 'unknown' end;

  if tg_op = 'UPDATE' then
    foreach col in array tg_argv loop
      old_v := old_j ->> col;
      new_v := new_j ->> col;
      if old_v is distinct from new_v then
        insert into public.audit_log(actor_id, actor_email, actor_kind, table_name,
               row_pk, operation, field_name, old_value, new_value,
               reason, request_id, source)
        values (a_id, a_mail, a_kind, tg_table_name, pk, 'UPDATE',
                col, old_v, new_v, a_rsn, a_req, a_src);
      end if;
    end loop;
  else
    insert into public.audit_log(actor_id, actor_email, actor_kind, table_name,
           row_pk, operation, old_value, new_value, reason, request_id, source)
    values (a_id, a_mail, a_kind, tg_table_name, pk, tg_op,
            case when tg_op = 'DELETE' then old_j::text end,
            case when tg_op = 'INSERT' then new_j::text end,
            a_rsn, a_req, a_src);
  end if;

  return coalesce(new, old);
end $$;

drop trigger if exists trg_audit_user_profiles on public.user_profiles;
create trigger trg_audit_user_profiles
  after insert or update or delete on public.user_profiles
  for each row execute function public.fn_audit_row_changes(
    'solis_station_id','full_name','phone','address','electricity_provider_id',
    'odoo_partner_id','odoo_email','odoo_customer_name');

drop trigger if exists trg_audit_solar_systems on public.solar_systems;
create trigger trg_audit_solar_systems
  after insert or update or delete on public.solar_systems
  for each row execute function public.fn_audit_row_changes(
    'user_id','system_name','capacity_kwp','installation_date',
    'battery_capacity_kwh','status','address','is_primary',
    'solis_station_id','solis_plant_name','solis_user_email',
    'odoo_lead_id','odoo_lead_email',
    -- added by file 15 (2026-09-19): the "manually verified" tick on a mapping
    'mapping_verified_at','mapping_verified_note');

drop trigger if exists trg_audit_staff_users on public.staff_users;
create trigger trg_audit_staff_users
  after insert or update or delete on public.staff_users
  for each row execute function public.fn_audit_row_changes(
    'email','role','active','revoked_at');

-- ------------------------------------------------------------------
-- HOW THE APPLICATION POPULATES THE GUCs — this is the one place PostgREST
-- hurts, because set_config needs a SQL round trip in the same session and
-- PostgREST gives you neither.
--
-- PREFERRED: have the back-office FastAPI service write through psycopg on a
-- real Postgres connection instead of PostgREST for mutations. One transaction:
--
--   select set_config('app.actor_id',    %s, true),
--          set_config('app.actor_email', %s, true),
--          set_config('app.reason',      %s, true),
--          set_config('app.request_id',  %s, true),
--          set_config('app.source',      'backoffice', true);
--   update public.solar_systems set ... where id = %s;
--   commit;
--
-- The third argument `true` makes the setting TRANSACTION-LOCAL, so it cannot
-- leak into the next user of a pooled connection. This also gives you real
-- transactions, which the multi-field edit + backfill enqueue needs.
--
-- NOTE: .env's DATABASE_URL is postgresql://postgres:***@localhost:5432/
-- microservices_db — that is NOT this Supabase project. You need the real
-- Supabase connection string (Dashboard -> Settings -> Database) before this
-- path works, and no pg driver is installed in .venv yet.
--
-- IF YOU KEEP POSTGREST: expose a single SECURITY DEFINER RPC that sets the
-- GUCs and performs the update in one statement. This project currently exposes
-- ZERO RPCs, so that would be the first — and it must
-- `revoke execute ... from anon` and check is_staff() internally.
--
-- Never let the application write the audit row itself as the ONLY mechanism:
-- an exception between the data write and the audit write leaves an unlogged
-- change. The trigger is in the same transaction, so it cannot desynchronise.
-- ------------------------------------------------------------------
