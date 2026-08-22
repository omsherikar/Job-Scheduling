create table users (
  id            uuid primary key default fl.uuidv7(),
  email         text not null unique,
  password_hash text not null,
  name          text,
  created_at    timestamptz not null default fl.now()
);

create table organizations (
  id         uuid primary key default fl.uuidv7(),
  name       text not null,
  created_at timestamptz not null default fl.now()
);

create table org_members (
  org_id  uuid not null references organizations(id) on delete cascade,
  user_id uuid not null references users(id) on delete cascade,
  role    text not null check (role in ('owner', 'admin', 'member', 'viewer')),
  primary key (org_id, user_id)
);

create table projects (
  id         uuid primary key default fl.uuidv7(),
  org_id     uuid not null references organizations(id) on delete cascade,
  name       text not null,
  created_at timestamptz not null default fl.now(),
  unique (org_id, name)
);

create table retry_policies (
  id                uuid primary key default fl.uuidv7(),
  project_id        uuid not null references projects(id) on delete cascade,
  name              text not null,
  kind              backoff_kind not null default 'exponential',
  max_attempts      int not null default 5 check (max_attempts between 1 and 50),
  max_lost_attempts int not null default 3 check (max_lost_attempts between 1 and 50),
  base_delay_ms     int not null default 1000 check (base_delay_ms > 0),
  max_delay_ms      int not null default 3600000 check (max_delay_ms > 0),
  jitter            boolean not null default true,
  unique (project_id, name),
  check (max_delay_ms >= base_delay_ms)
);

create table queues (
  id               uuid primary key default fl.uuidv7(),
  project_id       uuid not null references projects(id) on delete cascade,
  name             text not null,
  paused           boolean not null default false,
  retry_policy_id  uuid not null references retry_policies(id) on delete restrict,
  default_priority int not null default 0,

  max_concurrency int not null default 10 check (max_concurrency > 0),
  in_flight       int not null default 0 check (in_flight >= 0),

  max_depth    int check (max_depth is null or max_depth > 0),
  queued_depth int not null default 0 check (queued_depth >= 0),

  rl_limit_per_sec numeric not null default 0 check (rl_limit_per_sec >= 0),
  rl_burst         numeric not null default 0 check (rl_burst >= 0),
  rl_tokens        numeric not null default 0 check (rl_tokens >= 0),
  rl_refilled_at   timestamptz not null default fl.now(),

  breaker_state        breaker_state not null default 'closed',
  breaker_open_until   timestamptz,
  breaker_probe_budget int not null default 0,

  created_at timestamptz not null default fl.now(),
  unique (project_id, name),
  unique (project_id, id)
);

create table schedules (
  id             uuid primary key default fl.uuidv7(),
  queue_id       uuid not null references queues(id) on delete cascade,
  name           text not null,
  cron_expr      text not null,
  timezone       text not null default 'UTC',
  job_type       text not null,
  payload        jsonb not null default '{}',
  enabled        boolean not null default true,
  overlap_policy text not null default 'skip' check (overlap_policy in ('allow', 'skip')),
  catchup_policy text not null default 'skip' check (catchup_policy in ('skip', 'fire_once')),
  next_run_at    timestamptz not null,
  last_fired_for timestamptz,
  created_at     timestamptz not null default fl.now(),
  unique (queue_id, name)
);

create table workers (
  id              uuid primary key default fl.uuidv7(),
  project_id      uuid not null references projects(id) on delete cascade,
  hostname        text not null,
  pid             int not null,
  generation      int not null default 1,
  max_concurrency int not null check (max_concurrency > 0),
  state           text not null default 'active' check (state in ('active', 'draining', 'dead')),
  started_at      timestamptz not null default fl.now(),
  last_seen_at    timestamptz not null default fl.now()
);

create table jobs (
  id         uuid primary key default fl.uuidv7(),
  project_id uuid not null,
  queue_id   uuid not null,

  schedule_id   uuid references schedules(id) on delete set null,
  scheduled_for timestamptz,
  batch_id      uuid,

  type    text not null,
  payload jsonb not null default '{}',
  status  job_status not null default 'queued',

  priority int not null default 0,
  run_at   timestamptz not null default fl.now(),

  fence             bigint not null default 0,
  attempt_count     int not null default 0,
  replay_generation int not null default 0,
  snooze_count      int not null default 0,

  worker_id   uuid references workers(id) on delete restrict,
  deadline_at timestamptz,

  idempotency_key text,
  timeout_ms      int not null default 300000 check (timeout_ms > 0),
  retry_policy_id uuid not null references retry_policies(id) on delete restrict,
  pending_deps    int not null default 0 check (pending_deps >= 0),

  created_at  timestamptz not null default fl.now(),
  claimed_at  timestamptz,
  started_at  timestamptz,
  finished_at timestamptz,

  foreign key (project_id, queue_id) references queues (project_id, id) on delete cascade,
  check (pg_column_size(payload) <= 262144),
  check (schedule_id is null or scheduled_for is not null)
);

create table job_leases (
  job_id     uuid primary key references jobs(id) on delete cascade,
  fence      bigint not null,
  worker_id  uuid not null references workers(id) on delete restrict,
  expires_at timestamptz not null,
  progress   jsonb,
  updated_at timestamptz not null default fl.now()
);

create table job_executions (
  id                uuid primary key default fl.uuidv7(),
  job_id            uuid not null,
  queue_id          uuid not null,
  attempt           int not null check (attempt > 0),
  replay_generation int not null default 0,
  fence             bigint not null,
  worker_id         uuid not null references workers(id) on delete restrict,
  outcome           exec_outcome,
  error_class       text,
  error_message     text,
  error_stack       text,
  started_at        timestamptz not null default fl.now(),
  finished_at       timestamptz,
  duration_ms       bigint generated always as
    (((extract(epoch from (finished_at - started_at)) * 1000))::bigint) stored,
  unique (job_id, replay_generation, attempt)
);

create table job_logs (
  id           bigint generated always as identity,
  execution_id uuid not null,
  ts           timestamptz not null default fl.now(),
  level        text not null default 'info' check (level in ('debug', 'info', 'warn', 'error')),
  message      text not null,
  meta         jsonb,
  primary key (ts, id)
) partition by range (ts);

create table dead_letter_jobs (
  job_id            uuid primary key,
  queue_id          uuid not null,
  project_id        uuid not null,
  reason            text not null check (reason in
                      ('attempts_exhausted', 'permanent_error', 'lost_exhausted', 'timeout_exhausted')),
  last_error_class  text,
  last_error_message text,
  execution_history jsonb not null,
  dead_at           timestamptz not null default fl.now(),
  replayed_at       timestamptz,
  replayed_by       uuid references users(id) on delete set null
);

create table events (
  id         bigint generated always as identity,
  topic      text not null,
  entity_id  uuid not null,
  project_id uuid not null,
  payload    jsonb not null,
  created_at timestamptz not null default fl.now(),
  primary key (created_at, id)
) partition by range (created_at);
