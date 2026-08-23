drop index idx_jobs_queue_status;

create function fl.report_success(job uuid, fence bigint, exec uuid)
returns table (queue uuid) language plpgsql as $$
#variable_conflict use_variable
declare
  q   uuid;
  pr  uuid;
  dur bigint;
begin
  update jobs j set status = 'completed', finished_at = fl.now(), worker_id = null
   where j.id = job and j.fence = fence and j.status = 'running'
  returning j.queue_id, j.project_id into q, pr;

  if not found then
    return;
  end if;

  update job_executions x set outcome = 'success', finished_at = fl.now()
   where x.id = exec and x.finished_at is null
  returning x.duration_ms into dur;

  delete from job_leases where job_id = job;

  insert into queue_stats_minute (queue_id, minute, completed, duration_ms_sum)
  values (q, date_trunc('minute', fl.now()), 1, coalesce(dur, 0))
  on conflict (queue_id, minute) do update set
    completed       = queue_stats_minute.completed + excluded.completed,
    duration_ms_sum = queue_stats_minute.duration_ms_sum + excluded.duration_ms_sum;

  perform fl.queue_release(q, 1);

  perform fl.emit('job.completed', job, pr,
    jsonb_build_object('status', 'completed', 'outcome', 'success'));

  queue := q;
  return next;
end
$$;
