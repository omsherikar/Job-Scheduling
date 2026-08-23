create function fl.report_snooze(job uuid, fence bigint, exec uuid, delay_ms bigint)
returns table (queue uuid) language plpgsql as $$
#variable_conflict use_variable
declare
  q  uuid;
  pr uuid;
begin
  update jobs j set
    status = 'scheduled',
    fence = j.fence + 1,
    run_at = fl.now() + make_interval(secs =>
      greatest(least(delay_ms, rp.max_delay_ms::bigint), 0)::double precision / 1000),
    snooze_count = j.snooze_count + 1,
    worker_id = null,
    claimed_at = null,
    deadline_at = null
  from retry_policies rp
  where j.id = job and rp.id = j.retry_policy_id
    and j.status = 'running'
    and (fence is null or j.fence = fence)
    and j.snooze_count < rp.max_attempts
  returning j.queue_id, j.project_id into q, pr;

  if not found then
    return;
  end if;

  update job_executions x set outcome = 'snoozed', finished_at = fl.now()
   where x.id = exec and x.finished_at is null;

  delete from job_leases where job_id = job;

  perform fl.queue_release(q, 1);

  perform fl.emit('job.snoozed', job, pr, jsonb_build_object('delay_ms', delay_ms));

  queue := q;
  return next;
end
$$;
