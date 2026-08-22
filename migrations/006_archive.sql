create function fl.ensure_partition(parent text, day date) returns void
language plpgsql as $$
declare
  part text := parent || '_' || to_char(day, 'YYYYMMDD');
begin
  if to_regclass(part) is null then
    execute format('create table %I partition of %I for values from (%L) to (%L)',
                   part, parent, day, day + 1);
  end if;
end
$$;

create function fl.ensure_partitions(parent text, from_day date, to_day date) returns void
language plpgsql as $$
declare
  d date := from_day;
begin
  while d <= to_day loop
    perform fl.ensure_partition(parent, d);
    d := d + 1;
  end loop;
end
$$;

create function fl.drop_partitions_before(parent text, cutoff date) returns int
language plpgsql as $$
declare
  r record;
  dropped int := 0;
begin
  for r in
    select c.relname
      from pg_class c
      join pg_inherits i on i.inhrelid = c.oid
      join pg_class p on p.oid = i.inhparent
     where p.relname = parent
       and c.relname ~ '_[0-9]{8}$'
       and to_date(right(c.relname, 8), 'YYYYMMDD') < cutoff
  loop
    execute format('drop table %I', r.relname);
    dropped := dropped + 1;
  end loop;
  return dropped;
end
$$;

create table jobs_archive (
  like jobs including defaults,
  terminal_at timestamptz not null
) partition by range (terminal_at);

alter table jobs_archive add primary key (terminal_at, id);
create index idx_jobs_archive_id on jobs_archive using brin (id);
create index idx_jobs_archive_queue on jobs_archive using brin (queue_id);

create table job_executions_archive (
  id                uuid not null,
  job_id            uuid not null,
  queue_id          uuid not null,
  attempt           int not null,
  replay_generation int not null,
  fence             bigint not null,
  worker_id         uuid not null,
  outcome           exec_outcome,
  error_class       text,
  error_message     text,
  error_stack       text,
  started_at        timestamptz not null,
  finished_at       timestamptz not null,
  duration_ms       bigint,
  primary key (finished_at, id)
) partition by range (finished_at);

create index idx_exec_archive_job on job_executions_archive using brin (job_id);

create table job_logs_archive (
  id           bigint not null,
  execution_id uuid not null,
  ts           timestamptz not null,
  level        text not null,
  message      text not null,
  meta         jsonb,
  primary key (ts, id)
) partition by range (ts);

create index idx_logs_archive_exec on job_logs_archive using brin (execution_id);

select fl.ensure_partitions('events', (now() - interval '1 day')::date, (now() + interval '7 days')::date);
select fl.ensure_partitions('job_logs', (now() - interval '1 day')::date, (now() + interval '7 days')::date);
select fl.ensure_partitions('jobs_archive', (now() - interval '1 day')::date, (now() + interval '7 days')::date);
select fl.ensure_partitions('job_executions_archive', (now() - interval '1 day')::date, (now() + interval '7 days')::date);
select fl.ensure_partitions('job_logs_archive', (now() - interval '1 day')::date, (now() + interval '7 days')::date);
