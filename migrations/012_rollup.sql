drop index idx_jobs_archivable;

create index idx_jobs_archivable on jobs (finished_at)
  where status in ('completed', 'cancelled', 'dead_letter');

create index idx_exec_finished on job_executions (finished_at)
  where finished_at is not null;

create table dead_letter_jobs_archive (
  like dead_letter_jobs including defaults,
  archived_at timestamptz not null default fl.now()
) partition by range (dead_at);

alter table dead_letter_jobs_archive add primary key (dead_at, job_id);
