alter table jobs add constraint jobs_terminal_has_finished check (
  (status in ('completed', 'dead_letter', 'cancelled')) = (finished_at is not null)
);

alter table jobs add constraint jobs_active_has_owner check (
  case when status in ('claimed', 'running')
       then worker_id is not null and claimed_at is not null and deadline_at is not null
       else true end
);

alter table jobs add constraint jobs_running_has_started check (
  case when status = 'running' then started_at is not null else true end
);

alter table jobs add constraint jobs_idle_has_no_owner check (
  case when status in ('scheduled', 'queued', 'retry_wait')
       then worker_id is null else true end
);

alter table jobs add constraint jobs_scheduled_deps_or_future check (
  case when status = 'scheduled'
       then pending_deps > 0 or run_at > created_at else true end
);

alter table jobs add constraint jobs_fence_matches_attempts check (fence >= attempt_count);

alter table job_executions add constraint je_finished_after_started check (
  finished_at is null or finished_at >= started_at
);

alter table job_executions add constraint je_outcome_with_finish check (
  (outcome is null) = (finished_at is null)
);

alter table queues add constraint queues_open_has_deadline check (
  breaker_state <> 'open' or breaker_open_until is not null
);

alter table queues add constraint queues_probe_budget_sane check (
  breaker_probe_budget >= 0 and (breaker_state = 'half_open' or breaker_probe_budget = 0)
);

alter table queues add constraint queues_depth_cap check (
  max_depth is null or queued_depth <= max_depth
);

alter table queues add constraint queues_tokens_within_burst check (rl_tokens <= rl_burst);

alter table job_leases add constraint leases_fresh check (expires_at > '-infinity');

create table tz_names (name text primary key);
insert into tz_names select name from pg_timezone_names;

alter table schedules add constraint schedules_tz_known
  foreign key (timezone) references tz_names(name) on delete restrict;
