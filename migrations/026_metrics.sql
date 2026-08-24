create view fl.queue_age as
select q.id as queue_id, q.name, q.project_id, j.priority,
       extract(epoch from (fl.now() - min(j.run_at)))::int as oldest_ready_seconds,
       count(*)::int as ready
  from queues q
  join jobs j on j.queue_id = q.id
 where j.status = 'queued' and j.run_at <= fl.now()
 group by q.id, q.name, q.project_id, j.priority;

create view fl.fenced_writes as
select count(*)::bigint as total from job_fence_violations;

create view fl.reaper_lag as
select coalesce(max(extract(epoch from (fl.now() - l.expires_at))), 0)::int as seconds,
       count(*)::int as overdue
  from job_leases l
  join jobs j on j.id = l.job_id
 where j.status in ('claimed', 'running') and l.expires_at < fl.now();
