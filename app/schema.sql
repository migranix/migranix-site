-- ============================================================
-- Migranix Supabase Schema — Full SaaS
-- Run in Supabase SQL Editor: https://app.supabase.com
-- ============================================================

-- CONNECTIONS
create table if not exists connections (
    id uuid default gen_random_uuid() primary key,
    user_id uuid references auth.users(id) on delete cascade not null,
    name text not null,
    type text not null,
    credentials jsonb not null,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);
alter table connections enable row level security;
create policy "Users own connections" on connections using (auth.uid() = user_id);
create index if not exists idx_conn_user on connections(user_id);

-- QUERY HISTORY
create table if not exists query_history (
    id uuid default gen_random_uuid() primary key,
    user_id uuid references auth.users(id) on delete cascade not null,
    connection_id uuid references connections(id) on delete set null,
    query_text text not null,
    execution_time_ms integer,
    row_count integer,
    created_at timestamptz default now()
);
alter table query_history enable row level security;
create policy "Users own query history" on query_history using (auth.uid() = user_id);
create index if not exists idx_qh_user on query_history(user_id);

-- USER PLANS (billing)
create table if not exists user_plans (
    user_id uuid references auth.users(id) on delete cascade primary key,
    plan text default 'free' not null,
    stripe_customer_id text,
    stripe_subscription_id text,
    pipeline_limit integer default 2,
    source_limit integer default 3,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);
alter table user_plans enable row level security;
create policy "Users read own plan" on user_plans for select using (auth.uid() = user_id);

-- PIPELINES (persisted state)
create table if not exists pipelines (
    id uuid primary key,
    user_id uuid references auth.users(id) on delete cascade not null,
    name text not null,
    config jsonb default '{}',
    sf_creds_enc text default '',
    status text default 'created',
    run_count integer default 0,
    last_stats jsonb default '{}',
    last_run timestamptz,
    error text,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);
alter table pipelines enable row level security;
create policy "Users own pipelines" on pipelines using (auth.uid() = user_id);
create index if not exists idx_pipe_user on pipelines(user_id);

-- PIPELINE RUNS (history)
create table if not exists pipeline_runs (
    id uuid default gen_random_uuid() primary key,
    pipeline_id uuid references pipelines(id) on delete cascade,
    user_id uuid references auth.users(id) on delete cascade not null,
    status text not null,
    mode text,
    rows_loaded integer default 0,
    elapsed_seconds float,
    error text,
    logs text[] default '{}',
    started_at timestamptz default now(),
    finished_at timestamptz
);
alter table pipeline_runs enable row level security;
create policy "Users own runs" on pipeline_runs using (auth.uid() = user_id);
create index if not exists idx_runs_pipeline on pipeline_runs(pipeline_id);
create index if not exists idx_runs_user on pipeline_runs(user_id);

-- SCHEMA SNAPSHOTS (observability)
create table if not exists schema_snapshots (
    id uuid default gen_random_uuid() primary key,
    user_id uuid references auth.users(id) on delete cascade not null,
    pipeline_id uuid,
    table_name text not null,
    columns jsonb default '[]',
    row_count bigint default 0,
    captured_at timestamptz default now()
);
alter table schema_snapshots enable row level security;
create policy "Users own snapshots" on schema_snapshots using (auth.uid() = user_id);
create index if not exists idx_snap_pipeline on schema_snapshots(pipeline_id, captured_at desc);

-- DATA LINEAGE
create table if not exists data_lineage (
    id uuid default gen_random_uuid() primary key,
    user_id uuid references auth.users(id) on delete cascade not null,
    pipeline_id uuid,
    source_type text,
    source_table text,
    target_database text,
    target_schema text,
    target_table text,
    transformation text,
    created_at timestamptz default now()
);
alter table data_lineage enable row level security;
create policy "Users own lineage" on data_lineage using (auth.uid() = user_id);
create index if not exists idx_lineage_user on data_lineage(user_id, created_at desc);

-- ALERT CONFIGS
create table if not exists alert_configs (
    user_id uuid references auth.users(id) on delete cascade primary key,
    email text default '',
    on_failure boolean default true,
    on_success boolean default false,
    on_schema_drift boolean default true,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);
alter table alert_configs enable row level security;
create policy "Users own alerts" on alert_configs using (auth.uid() = user_id);
