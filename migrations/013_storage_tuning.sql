create index idx_jobs_queue_status on jobs (queue_id, status);

create index idx_jobs_queued_age on jobs (created_at) where status = 'queued';

create index idx_dlq_list on dead_letter_jobs (project_id, dead_at desc, job_id desc);

create index idx_worker_queues_queue on worker_queues (queue_id, worker_id);

create index idx_qsm_unrolled on queue_stats_minute (queue_id, minute)
  where duration_ms_p50 is null;

drop index idx_exec_job;
drop index idx_jobs_deadline;
drop index idx_queues_project;
drop index idx_projects_org;

alter table jobs set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);
alter table job_executions set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);
alter table job_leases set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000);
