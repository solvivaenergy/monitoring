-- Stores dashboard/system-wide counters (e.g., total clients) in Supabase.
-- Run this once in the Supabase SQL editor.
-- After this succeeds, run:
--   python -m api.sync_client_counts --apply

create table if not exists public.system_metrics (
  metric_name text primary key,
  metric_value bigint not null,
  metadata jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create index if not exists system_metrics_updated_at_idx on public.system_metrics (updated_at desc);

alter table public.system_metrics enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'system_metrics'
      and policyname = 'service role full access'
  ) then
    create policy "service role full access"
      on public.system_metrics
      for all
      using (auth.role() = 'service_role')
      with check (auth.role() = 'service_role');
  end if;
end $$;