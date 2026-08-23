create function fl.report_success_batch(ids uuid[], fences bigint[], execs uuid[])
returns table (job uuid) language plpgsql as $$
declare
  done_ids   uuid[];
  done_execs uuid[];
begin
  with input as (
    select unnest(ids) as id, unnest(fences) as fence, unnest(execs) as exec
  ),
  upd as (
    update jobs j set status = 'completed', finished_at = fl.now(), worker_id = null
      from input i
     where j.id = i.id and j.fence = i.fence and j.status = 'running'
    returning j.id, i.exec
  )
  select array_agg(id), array_agg(exec) into done_ids, done_execs from upd;

  if done_ids is null then
    return;
  end if;

  update job_executions set outcome = 'success', finished_at = fl.now()
   where id = any(done_execs) and finished_at is null;

  delete from job_leases where job_id = any(done_ids);

  insert into queue_stats_minute (queue_id, minute, completed, duration_ms_sum)
  select x.queue_id, date_trunc('minute', fl.now()), count(*)::int, coalesce(sum(x.duration_ms), 0)
    from job_executions x
   where x.id = any(done_execs)
   group by x.queue_id
  on conflict (queue_id, minute) do update set
    completed       = queue_stats_minute.completed + excluded.completed,
    duration_ms_sum = queue_stats_minute.duration_ms_sum + excluded.duration_ms_sum;

  perform fl.queue_release(t.queue_id, t.n)
     from (select queue_id, count(*)::int as n from jobs
            where id = any(done_ids) group by queue_id order by queue_id) t;

  perform fl.emit('job.completed', j.id, j.project_id,
                  jsonb_build_object('status', 'completed', 'outcome', 'success'))
     from (select id, project_id from jobs
            where id = any(done_ids) order by project_id, id) j;

  return query select unnest(done_ids);
end
$$;
