create index idx_jobs_claimable on jobs (queue_id, priority desc, run_at asc, id asc)
  where status = 'queued';

create index idx_jobs_inflight on jobs (queue_id)
  where status in ('claimed', 'running');

create index idx_jobs_promote on jobs (run_at)
  where status in ('scheduled', 'retry_wait') and pending_deps = 0;

create index idx_jobs_deadline on jobs (deadline_at)
  where status in ('claimed', 'running');

create unique index uq_jobs_idem on jobs (queue_id, idempotency_key)
  where idempotency_key is not null;

create unique index uq_jobs_schedule_tick on jobs (schedule_id, scheduled_for)
  where schedule_id is not null;

create index idx_jobs_list on jobs (project_id, status, created_at desc, id desc);
create index idx_jobs_type on jobs (queue_id, type);
create index idx_jobs_batch on jobs (batch_id) where batch_id is not null;
create index idx_jobs_worker on jobs (worker_id) where worker_id is not null;
create index idx_jobs_schedule on jobs (schedule_id) where schedule_id is not null;
create index idx_jobs_policy on jobs (retry_policy_id);

create index idx_leases_expiry on job_leases (expires_at);
create index idx_leases_worker on job_leases (worker_id);

create unique index uq_exec_one_open on job_executions (job_id) where finished_at is null;
create index idx_exec_job on job_executions (job_id, replay_generation, attempt);
create index idx_exec_worker on job_executions (worker_id);
create index idx_exec_breaker on job_executions (queue_id, finished_at desc)
  where outcome is not null;

create index idx_logs_execution on job_logs (execution_id, ts);

create index idx_dlq_queue on dead_letter_jobs (queue_id, dead_at desc);
create index idx_dlq_unreplayed on dead_letter_jobs (queue_id, dead_at)
  where replayed_at is null;

create index idx_events_tail on events (project_id, id);

create index idx_queues_project on queues (project_id);
create index idx_queues_policy on queues (retry_policy_id);
create index idx_policies_project on retry_policies (project_id);
create index idx_schedules_queue on schedules (queue_id);
create index idx_schedules_due on schedules (next_run_at) where enabled;
create index idx_workers_project on workers (project_id, state);
create index idx_projects_org on projects (org_id);
create index idx_org_members_user on org_members (user_id);
