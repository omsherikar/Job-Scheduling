create table failure_summaries (
  queue_id     uuid primary key references queues(id) on delete cascade,
  digest       text not null,
  summary      text not null,
  model        text not null,
  generated_at timestamptz not null default fl.now()
);

create function fl.failure_digest(q uuid, h int) returns text
language sql stable as $$
  select coalesce(md5(string_agg(class || ':' || n, ',' order by class)), '')
    from (
      select coalesce(x.error_class, 'unknown') as class, count(*)::text as n
        from job_executions x
       where x.queue_id = q
         and x.outcome in ('retryable_error', 'permanent_error', 'timeout', 'lost')
         and x.finished_at >= fl.now() - make_interval(hours => h)
       group by 1
    ) g
$$;
