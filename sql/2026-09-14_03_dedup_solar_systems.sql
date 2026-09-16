-- 2026-09-14 step 03 — collapse the 104 duplicate solar_systems pairs.
-- Run this once in the Supabase SQL editor. Order: after 02, before 04.
-- Re-runnable: a second run finds 0 duplicates and is a no-op.
--
-- *** HEAVY LOCK WARNING ***
-- The two UPDATE ... SET system_id statements take ROW EXCLUSIVE on the reading
-- tables and touch 5,617 + ~20,132 rows. They are fast (both tables are ~25 MB
-- and fully in shared buffers) but they run inside one transaction with the
-- DELETE. Run this in a cron gap. Do NOT run it at 18:00 UTC.
--
-- WHY THESE ROWS EXIST (root cause, corrected against the grounding facts):
-- they are NOT a one-off "buggy backfill on 2026-08-10". They are a LIVE,
-- RECURRING RACE between two crons, and it will fire again on the next
-- onboarding batch. In all 104 pairs the older row has address '-' (ASCII
-- hyphen) and the newer '—' (U+2014), 104/104 with no exceptions. Those two
-- strings have exactly one writer each:
--     '-'  -> api/sync_five_minutes_to_supabase.py:266  (_ensure_active_system)
--     '—'  -> api/sync_to_supabase.py:256, backfill_history.py:285,
--             backfill_newly_onboarded.py:209 and :396
-- The daily cron fires at 18:00 UTC; the 15-minute cron at 18:00 and 18:15.
-- Both do "find an active solar_systems row for this user, else INSERT", with
-- no unique constraint and no ON CONFLICT guard. Pairs were created on
-- 2026-08-20 (91 pairs) and 2026-08-10 (13 pairs) — the two nights a batch was
-- onboarded. File 04 closes the hole with UNIQUE(user_id, solis_station_id).
--
-- WHY REPOINT BEFORE DELETE (this ordering is not optional):
--   101 of the 104 users have energy_readings on BOTH rows of their pair.
--   The two rows are complementary TEMPORAL halves of one station's history
--   (one holds ~2026-06-22..08-31, the other 09-01..09-14); overlapping
--   calendar days = 0, forced by the existing UNIQUE(user_id, timestamp).
--   So deleting either row destroys real data. energy_readings.system_id is a
--   real NOT NULL FK to solar_systems.id whose ON DELETE rule is not visible
--   over PostgREST — if it is CASCADE, deleting first silently destroys 7,794
--   readings. Repointing first is correct under every possible rule.
--
-- Survivor rule: MIN(created_at), ties broken by id. Deterministic. Do NOT pick
-- by reading count — which row holds the LATEST reading is unpredictable
-- (older row 90, newer 11), so "newest row" and "row with recent data" disagree
-- on ~90 of the 104 users. Either choice is safe only because we repoint.
--
-- EXPECTED (measured live 2026-09-14, exact):
--   rows_to_delete                       104
--   energy_readings repointed          5,617
--   energy_readings_five_minutes    ~20,132  (drifts; rolling 1-day table)
--   cleaned_data repointed                 0  (its data predates the pairs;
--                                              covered anyway, it has no FK and
--                                              is the likeliest thing forgotten)

-- *** HOW TO RUN (revised 2026-09-15) ***
-- Paste this ENTIRE file and run it as ONE submission. It is a single DO block
-- plus read-only checks, so it does not matter whether your editor wraps the
-- submission in a transaction or splits it on semicolons.
--
-- The earlier version of this file used `create temp table ... on commit drop`
-- across several statements. The Supabase SQL editor splits a submission on
-- semicolons and commits between statements, so the temp table was dropped the
-- instant it was created and the next statement failed with
--     42P01: relation "_keep" does not exist
-- Everything now happens inside one DO block, which is one transaction whatever
-- the editor does. A failed check RAISEs, which rolls the whole thing back.

do $$
declare
  v_to_delete      int;
  v_er_repointed   int;
  v_5m_repointed   int;
  v_cd_repointed   int;
  v_deleted        int;
  v_still_dupes    int;
  v_orphans        int;
begin
  -- Re-runnable after a failed attempt. A RAISE rolls the transaction back, so
  -- these should never exist -- but a session-scoped temp table that survived
  -- would otherwise make every retry fail with "relation _keep already exists",
  -- which looks alarming and is not the real problem.
  drop table if exists _keep;
  drop table if exists _drop;

  -- Survivor per DUPLICATE FINGERPRINT, not per user. See the note above: one
  -- user (Richard Bartolome) legitimately owns two DIFFERENT systems today
  -- ("Commercial" 11.05 kWp with 451 readings, "Residential" 5.72 kWp), and a
  -- user_id-scoped rule would delete one of them. Verified live 2026-09-15:
  -- user_id rule => 105 rows, fingerprint rule => 104 rows.
  create temp table _keep as
  select distinct on (user_id, system_name, capacity_kwp)
         user_id, system_name, capacity_kwp, id as keep_id
    from public.solar_systems
   order by user_id, system_name, capacity_kwp, created_at asc, id asc;

  create temp table _drop as
  select s.id as drop_id, k.keep_id, s.installation_date as drop_install
    from public.solar_systems s
    join _keep k
      on k.user_id      = s.user_id
     and k.system_name  is not distinct from s.system_name
     and k.capacity_kwp is not distinct from s.capacity_kwp
   where s.id <> k.keep_id;

  select count(*) into v_to_delete from _drop;
  raise notice 'rows_to_delete = % (expect 104; 0 on a re-run)', v_to_delete;

  -- Preserve the real installation date before discarding the loser. The newer
  -- row is pinned to the synthetic default 2026-06-22 in 91 cases.
  update public.solar_systems s
     set installation_date = least(s.installation_date, d.drop_install)
    from _drop d
   where s.id = d.keep_id
     and d.drop_install is not null
     and d.drop_install < s.installation_date;

  -- Repoint BEFORE deleting. 101 of the 104 pairs carry readings on BOTH rows,
  -- and the FK's ON DELETE rule is not visible over PostgREST -- if it is
  -- CASCADE, deleting first silently destroys 7,794 readings.
  update public.energy_readings r
     set system_id = d.keep_id
    from _drop d
   where r.system_id = d.drop_id;
  get diagnostics v_er_repointed = row_count;

  update public.energy_readings_five_minutes r
     set system_id = d.keep_id
    from _drop d
   where r.system_id = d.drop_id;
  get diagnostics v_5m_repointed = row_count;

  update public.cleaned_data r
     set system_id = d.keep_id
    from _drop d
   where r.system_id = d.drop_id;
  get diagnostics v_cd_repointed = row_count;

  delete from public.solar_systems s
   using _drop d
   where s.id = d.drop_id;
  get diagnostics v_deleted = row_count;

  raise notice 'energy_readings repointed        = % (expect 5617)', v_er_repointed;
  raise notice 'energy_readings_5m repointed     = % (expect ~20132, drifts)', v_5m_repointed;
  raise notice 'cleaned_data repointed           = % (expect 0)', v_cd_repointed;
  raise notice 'solar_systems deleted            = %', v_deleted;

  -- Remaining FINGERPRINT duplicates. Deliberately not "users with >1 system":
  -- after this runs, exactly one user (Richard Bartolome) still holds two
  -- systems and that is the correct end state.
  select count(*) into v_still_dupes
    from (select user_id, system_name, capacity_kwp
            from public.solar_systems
           group by user_id, system_name, capacity_kwp
          having count(*) > 1) q;

  select count(*) into v_orphans
    from public.energy_readings r
    left join public.solar_systems s on s.id = r.system_id
   where s.id is null;

  if v_still_dupes <> 0 then
    raise exception 'ABORTED: % fingerprint duplicate group(s) remain', v_still_dupes;
  end if;
  if v_orphans <> 0 then
    raise exception 'ABORTED: % orphaned energy_readings', v_orphans;
  end if;

  raise notice 'still_dupes = 0, orphaned_readings = 0 -- committing.';

  drop table _keep;
  drop table _drop;
end $$;

-- Read-only confirmation. Run after the block above reports success.
select count(*)                                   as solar_systems_rows,
       count(distinct user_id)                    as distinct_customers,
       count(*) - count(distinct user_id)         as users_holding_extra_systems
  from public.solar_systems;
-- Expect: 615 rows, 614 customers, 1 extra -- that 1 is Richard Bartolome's
-- genuine second property, NOT a duplicate.

select count(*) as orphaned_readings
  from public.energy_readings r
  left join public.solar_systems s on s.id = r.system_id
 where s.id is null;
-- Expect 0.
