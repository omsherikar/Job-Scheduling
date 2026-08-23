alter table jobs_archive drop column idempotency_key;

create index idx_jobs_archivable on jobs (finished_at)
  where status in ('completed', 'cancelled');
