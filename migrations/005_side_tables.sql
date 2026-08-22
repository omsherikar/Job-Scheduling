create table job_transitions (
  from_status job_status not null,
  to_status   job_status not null,
  note        text not null,
  primary key (from_status, to_status)
);

insert into job_transitions (from_status, to_status, note) values
  ('scheduled',  'queued',      'promoter, due or deps met'),
  ('scheduled',  'cancelled',   'api'),
  ('queued',     'claimed',     'worker claim'),
  ('queued',     'cancelled',   'api'),
  ('claimed',    'running',     'worker start'),
  ('claimed',    'queued',      'drain release, no attempt burned'),
  ('claimed',    'retry_wait',  'reaper, lease expired before start'),
  ('claimed',    'cancelled',   'api'),
  ('claimed',    'dead_letter', 'reaper, attempts exhausted'),
  ('running',    'completed',   'worker success'),
  ('running',    'retry_wait',  'worker failure with attempts left'),
  ('running',    'dead_letter', 'permanent error or attempts exhausted'),
  ('running',    'cancelled',   'api'),
  ('running',    'queued',      'drain release past deadline'),
  ('running',    'scheduled',   'snooze'),
  ('retry_wait', 'queued',      'promoter, backoff elapsed'),
  ('retry_wait', 'cancelled',   'api'),
  ('retry_wait', 'dead_letter', 'breaker or manual'),
  ('dead_letter','queued',      'replay');

create function jobs_fsm() returns trigger language plpgsql as $$
begin
  if new.fence < old.fence then
    raise exception 'fence went backwards on job %: % -> %', old.id, old.fence, new.fence;
  end if;

  if old.status is distinct from new.status
     and not exists (select 1 from job_transitions
                      where from_status = old.status and to_status = new.status) then
    raise exception 'illegal transition % -> % on job %', old.status, new.status, old.id;
  end if;

  return new;
end
$$;

create trigger jobs_fsm before update on jobs
  for each row when (old.status is distinct from new.status or old.fence is distinct from new.fence)
  execute function jobs_fsm();

create function je_no_reclose() returns trigger language plpgsql as $$
begin
  if old.finished_at is not null then
    raise exception 'execution % is already closed as %', old.id, old.outcome;
  end if;
  return new;
end
$$;

create trigger je_no_reclose before update on job_executions
  for each row when (old.finished_at is not null)
  execute function je_no_reclose();

create table worker_queues (
  worker_id uuid not null references workers(id) on delete cascade,
  queue_id  uuid not null references queues(id) on delete cascade,
  primary key (worker_id, queue_id)
);

create table idempotency_keys (
  queue_id    uuid not null references queues(id) on delete cascade,
  key         text not null,
  job_id      uuid not null,
  fingerprint text not null,
  created_at  timestamptz not null default fl.now(),
  expires_at  timestamptz not null default fl.now() + interval '24 hours',
  primary key (queue_id, key)
);

create index idx_idem_expiry on idempotency_keys (expires_at);

create table job_fence_violations (
  id           bigint generated always as identity primary key,
  job_id       uuid not null,
  worker_id    uuid not null,
  attempted    text not null,
  held_fence   bigint not null,
  actual_fence bigint,
  actual_status job_status,
  noticed_at   timestamptz not null default fl.now()
);

create index idx_violations_job on job_fence_violations (job_id, noticed_at desc);

create table queue_stats_minute (
  queue_id     uuid not null references queues(id) on delete cascade,
  minute       timestamptz not null,
  completed    int not null default 0,
  failed       int not null default 0,
  dead_lettered int not null default 0,
  duration_ms_sum bigint not null default 0,
  duration_ms_p50 int,
  duration_ms_p95 int,
  primary key (queue_id, minute)
);

create table audit_log (
  id         bigint generated always as identity primary key,
  project_id uuid not null,
  actor_id   uuid references users(id) on delete set null,
  action     text not null,
  entity     text not null,
  entity_id  uuid,
  detail     jsonb,
  at         timestamptz not null default fl.now()
);

create index idx_audit_project on audit_log (project_id, at desc);

create table sessions (
  id         uuid primary key default fl.uuidv7(),
  user_id    uuid not null references users(id) on delete cascade,
  issued_at  timestamptz not null default fl.now(),
  expires_at timestamptz not null,
  revoked_at timestamptz
);

create index idx_sessions_user on sessions (user_id) where revoked_at is null;
