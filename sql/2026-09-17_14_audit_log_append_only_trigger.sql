-- 2026-09-17 step 14 — audit_log append-only via TRIGGER, not RULES.
-- Run over the direct connection (autocommit). Re-runnable. No heavy locks.
--
-- Migration 08 made audit_log append-only with two rewrite rules:
--     create rule audit_log_no_update as on update to audit_log do instead nothing;
--     create rule audit_log_no_delete as on delete to audit_log do instead nothing;
-- That broke something far away: audit_log.actor_id references auth.users(id)
-- ON DELETE SET NULL, and Postgres performs that action as an internal
--     UPDATE audit_log SET actor_id = NULL WHERE actor_id = <user>
-- The rule rewrote it into nothing, the RI machinery got no result back, and
-- every attempt to delete ANY auth user — customer or staff, via Supabase's
-- admin API or plain SQL — failed with
--     referential integrity query on "users" from constraint
--     "audit_log_actor_id_fkey" on "audit_log" gave unexpected result
--     HINT: This is most likely due to a rule having rewritten the query.
-- Found 2026-09-17 when the back office's "Delete staff" returned 500 from
-- GoTrue ("Database error deleting user"). It failed even for a user with zero
-- audit rows: the rewritten statement returns nothing regardless.
--
-- A trigger does not rewrite anything, so RI works. It refuses every DELETE and
-- every UPDATE except the one the FK performs: actor_id going to NULL with every
-- other column unchanged. actor_email stays, so the trail still says who acted.

drop rule if exists audit_log_no_update on public.audit_log;
drop rule if exists audit_log_no_delete on public.audit_log;

create or replace function public.tg_audit_log_append_only()
returns trigger
language plpgsql
as $$
begin
  if tg_op = 'DELETE' then
    raise exception 'audit_log is append-only (delete refused)';
  end if;
  -- The single permitted update: the FK's ON DELETE SET NULL of actor_id.
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

-- Verify: no rules left, one trigger present.
select (select count(*) from pg_rules where tablename = 'audit_log') as rules_left,
       (select count(*) from pg_trigger where tgrelid = 'public.audit_log'::regclass
          and tgname = 'trg_audit_log_append_only') as trigger_present;
