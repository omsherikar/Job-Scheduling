alter table queues drop constraint queues_depth_cap;
alter table queues drop column queued_depth;

drop index uq_jobs_idem;
alter table jobs drop column idempotency_key;
