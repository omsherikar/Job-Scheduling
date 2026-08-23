create table job_dependencies (
  parent_id uuid not null references jobs(id) on delete cascade,
  child_id  uuid not null references jobs(id) on delete cascade,
  primary key (parent_id, child_id),
  check (parent_id <> child_id)
);

create index idx_deps_child on job_dependencies (child_id);

alter table jobs add constraint jobs_deps_only_while_scheduled
  check (pending_deps = 0 or status = 'scheduled');

alter table workers add column handlers text[] not null default '{}';

alter type exec_outcome add value 'snoozed';
