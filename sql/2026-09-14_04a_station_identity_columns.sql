-- ============================================================================
-- FILE 04 IS SPLIT INTO THREE PARTS: 04a -> 04b -> 04c. Run them in order.
--
-- Why: the Supabase SQL editor runs a submission inside a transaction, and
-- CREATE INDEX CONCURRENTLY cannot run inside one (ERROR 25001). 04b holds
-- every CONCURRENTLY statement and must be run ONE STATEMENT AT A TIME; 04a
-- and 04c contain none and paste whole.
-- ============================================================================

-- 2026-09-14 step 04 — make solar_systems a first-class station, add the
-- back-office identity columns, and install the user_profiles compat mirror.
-- Run this once in the Supabase SQL editor. Order: after 03, before 05.
-- Fully idempotent and re-runnable.
--
-- *** HEAVY LOCK WARNING ***
--   * ALTER TABLE ... ADD COLUMN and ALTER COLUMN ... DROP NOT NULL each take a
--     brief ACCESS EXCLUSIVE lock. All are catalog-only and effectively
--     instant on PG11+ (no table rewrite; the one column with a default,
--     is_primary, uses a non-volatile default and is also instant).
--   * The CREATE UNIQUE INDEX CONCURRENTLY statements in PART 5 must each be
--     run ALONE — see the note in file 01.
--
-- FIELD PLACEMENT — decided per field, from the three-way-mapping audit:
--
--   ON THE STATION (solar_systems), because a customer owns N of them:
--     solis_station_id   MASTER DATA. This mapping is our decision, not a
--                        mirror of anything. UNIQUE. This is the single
--                        invariant that stops two customers being pointed at
--                        one plant.
--     solis_plant_name   CACHED MIRROR of Solis station_detail().stationName.
--     solis_user_email   CACHED MIRROR of Solis station_detail().userEmail.
--                        Only 579/648 stations have it -> 58 of your mapped
--                        stations WILL render blank. Design the cell for empty
--                        and never let a blank sync overwrite a hand-typed one.
--     odoo_lead_id       CACHED FK to crm.lead.id. Per-STATION, because a lead
--                        is per-project: Arnel Cipriano Chavez has 3 leads and
--                        1 partner. UNIQUE (one lead cannot describe two
--                        stations) — but note 3 station ids are each claimed by
--                        2 leads, so choosing the lead is sometimes a decision
--                        the operator makes, not a lookup.
--     odoo_lead_email    CACHED MIRROR of crm.lead.email_from (per-lead; can
--                        differ across one customer's leads).
--     odoo_lead_name     CACHED MIRROR of crm.lead.name, for operator context.
--     odoo_stage         CACHED MIRROR of crm.lead.stage_id[1].
--
--   ON THE USER (user_profiles), because it is one fact about a person:
--     odoo_partner_id    CACHED FK to res.partner.id. THE stable customer key:
--                        present on 633/634 station-bearing leads and already
--                        survives 1-customer-to-N-stations (8-9 partners own
--                        2-3 stations). NOT unique — two Supabase logins still
--                        map to one partner today because of the split-account
--                        workaround; it becomes unique only after identity
--                        merges. Plain index.
--     odoo_email         CACHED MIRROR of res.partner.email.
--     odoo_customer_name CACHED MIRROR of crm.lead.partner_id[1].
--
--   DELIBERATELY NOT CREATED — solis_customer_name.
--     Solis has NO customer-name field. station_detail() returns 389 keys with
--     zero owner/person-name fields, and /v1/api/userDetail returns HTTP 404.
--     `stationName` IS the only name Solis holds. Creating both columns would
--     guarantee two columns holding one value that drift apart. The back office
--     must render ONE column, labelled "Solis plant name". The user asked for
--     seven columns; Solis can supply six distinct facts.
--
--   NOT STORED AT ALL — the Supabase login email.
--     It lives in auth.users.email, is not reachable over PostgREST, and is a
--     THIRD value that disagrees with the Odoo email on 68/608 accounts and
--     with the Solis email on 149/548. Read it through the auth admin API.
--     Changing it changes what the customer signs in with and needs an explicit
--     confirm dialog, never an inline grid edit.
--
--   EVERY cached mirror is timestamped: solis_synced_at / odoo_synced_at. Solis
--   is flaky enough (timeouts, 502s, _MAX_RETRIES=3 is not enough) that a grid
--   fanning out 611 live station_detail calls will time out. Refresh nightly
--   from list_stations (7 calls for all 648) and fetch live only on Validate.

---------------------------------------------------------------------------
-- PART 1 — shared updated_at helper (an updated_at trigger is inferred to
-- exist already on user_profiles/solar_systems; new tables get one explicitly)
---------------------------------------------------------------------------
create or replace function public.tg_set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end $$;

---------------------------------------------------------------------------
-- PART 2 — station columns
---------------------------------------------------------------------------
alter table public.solar_systems
  add column if not exists solis_station_id     text,
  add column if not exists solis_plant_name     text,
  add column if not exists solis_user_email     text,
  add column if not exists solis_synced_at      timestamptz,
  add column if not exists solis_validated_at   timestamptz,
  add column if not exists solis_validation     text,
  add column if not exists odoo_lead_id         bigint,
  add column if not exists odoo_lead_email      text,
  add column if not exists odoo_lead_name       text,
  add column if not exists odoo_stage           text,
  add column if not exists odoo_synced_at       timestamptz,
  add column if not exists is_primary           boolean not null default false,
  add column if not exists last_backfill_at     timestamptz,
  add column if not exists last_backfill_status text;

comment on column public.solar_systems.solis_station_id is
  'Master data. The Solis plant this system IS. Unique fleet-wide.';
comment on column public.solar_systems.solis_plant_name is
  'Cached mirror of Solis station_detail().stationName. Solis has no separate '
  'customer-name field; this is also what the back office shows as the Solis '
  'name for the customer. Freshness: solis_synced_at.';
comment on column public.solar_systems.solis_user_email is
  'Cached mirror of Solis station_detail().userEmail. Blank for ~58 mapped '
  'stations. Never overwrite a non-null value with a blank sync.';
comment on column public.solar_systems.odoo_lead_id is
  'Cached FK to Odoo crm.lead.id. Per-station: a lead is per-project.';

-- Validation outcome must distinguish "verified absent" from "could not check".
-- Solis station_detail() returns None (does NOT raise) for a nonexistent id,
-- and raises SolisCloudError code 1 for an empty string. A validate button that
-- reports "does not exist" during a 502 will make an engineer blank correct data.
do $$
begin
  if not exists (select 1 from pg_constraint
                  where conrelid = 'public.solar_systems'::regclass
                    and conname  = 'solar_systems_solis_validation_chk') then
    alter table public.solar_systems
      add constraint solar_systems_solis_validation_chk
      check (solis_validation is null
             or solis_validation in ('verified','absent','unverifiable'));
  end if;
end $$;

-- status is 'active' on all 719 rows, so every `.eq("status","active")` filter
-- in the codebase is a no-op today. Give it a real lifecycle so decommissioned
-- stations can stop being backfilled. NOT VALID: adopts without a scan.
do $$
begin
  if not exists (select 1 from pg_constraint
                  where conrelid = 'public.solar_systems'::regclass
                    and conname  = 'solar_systems_status_chk') then
    alter table public.solar_systems
      add constraint solar_systems_status_chk
      check (status in ('active','inactive','decommissioned','pending')) not valid;
  end if;
end $$;

---------------------------------------------------------------------------
-- PART 3 — user columns
---------------------------------------------------------------------------
alter table public.user_profiles
  add column if not exists odoo_partner_id    bigint,
  add column if not exists odoo_email         text,
  add column if not exists odoo_customer_name text,
  add column if not exists odoo_synced_at     timestamptz;

comment on column public.user_profiles.odoo_partner_id is
  'Cached FK to Odoo res.partner.id — the stable customer identity that '
  'survives multi-station. NOT unique: split-account customers still map two '
  'Supabase logins to one partner until identities are merged.';
comment on column public.user_profiles.solis_station_id is
  'DEPRECATED COMPAT MIRROR of the user''s is_primary solar_systems row. '
  'Maintained by trigger. Live crons and the mobile app still read it '
  '(app_routes.py:74, sync_five_minutes:301, backfill_history:204, '
  'solviva_mcp:95,448). Do not drop until those are migrated.';

---------------------------------------------------------------------------
-- PART 4 — seed the station id from today's single-station mapping
---------------------------------------------------------------------------
-- Guarded: only fills NULLs, so a re-run cannot clobber an edit.
update public.solar_systems s
   set solis_station_id = nullif(btrim(p.solis_station_id), '')
  from public.user_profiles p
 where p.id = s.user_id
   and s.solis_station_id is null
   and nullif(btrim(p.solis_station_id), '') is not null;

-- MUST be 0 before PART 5 builds the unique index. If it is not, file 03 did
-- not run. (Simulated live: 615 surviving systems, 609 get a station id,
-- 609 distinct, 0 duplicates, 0 format violations.)
select count(*) as duplicate_station_ids
  from (select solis_station_id
          from public.solar_systems
         where solis_station_id is not null
         group by 1 having count(*) > 1) q;

-- Format guard. Real Solis ids are 19 digits; user_profiles currently holds the
-- fat-finger value '12' (Amelita Cherry Canta) — exactly what the validate
-- button is for. That profile has no solar_systems row so it is NOT seeded
-- above, which is why the station-side constraint can be validated immediately
-- while the profile-side one stays NOT VALID until someone fixes the row.
do $$
begin
  if not exists (select 1 from pg_constraint
                  where conrelid = 'public.solar_systems'::regclass
                    and conname  = 'solar_systems_solis_station_id_fmt_chk') then
    alter table public.solar_systems
      add constraint solar_systems_solis_station_id_fmt_chk
      check (solis_station_id is null or solis_station_id ~ '^[0-9]{15,20}$')
      not valid;
  end if;
  -- validate only if clean, so the file stays re-runnable either way
  if not exists (select 1 from public.solar_systems
                  where solis_station_id is not null
                    and solis_station_id !~ '^[0-9]{15,20}$') then
    alter table public.solar_systems
      validate constraint solar_systems_solis_station_id_fmt_chk;
  else
    raise notice 'solar_systems has non-conforming station ids; constraint left NOT VALID';
  end if;

  if not exists (select 1 from pg_constraint
                  where conrelid = 'public.user_profiles'::regclass
                    and conname  = 'user_profiles_solis_station_id_fmt_chk') then
    alter table public.user_profiles
      add constraint user_profiles_solis_station_id_fmt_chk
      check (solis_station_id is null or solis_station_id ~ '^[0-9]{15,20}$')
      not valid;   -- stays NOT VALID: the '12' row is real and must be triaged
  end if;
end $$;

-- address is NOT NULL today, which is the ONLY reason the '-' / '—' placeholder
-- junk exists at all (325 placeholder rows survive the dedup). Make it nullable
-- and clear them: a real address belongs on the station and should come from
-- Odoo x_studio_complete_address, not from a cron's filler string.
-- *** HEAVY LOCK: brief ACCESS EXCLUSIVE, catalog-only, instant. ***
alter table public.solar_systems alter column address drop not null;

update public.solar_systems
   set address = null
 where address in ('-', '—', '–', '');

---------------------------------------------------------------------------
-- PART 5 — uniqueness. This is what turns the Validate button from a TOCTOU
-- race into a guarantee. Today NOTHING enforces one-customer-per-station; the
-- "zero duplicate station ids" fact is true of the data, not of the schema.
-- (Corroboration that this matters: referral_code, equally unconstrained,
-- already has 2 duplicate values and phone has 3.)
--
-- *** RUN EACH OF THE FOUR STATEMENTS BELOW ALONE — CONCURRENTLY. ***
---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- STOP. Check the query above returned duplicate_station_ids = 0.
-- If it did not, file 03 did not complete -- do NOT run 04b, its unique index
-- will fail. Expect 615 systems, 609 seeded with a station id, 609 distinct.
-- Then run 04b, ONE STATEMENT AT A TIME.
-- ---------------------------------------------------------------------------
