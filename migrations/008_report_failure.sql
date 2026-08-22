create function fl.report_failure(
  job           uuid,
  fence         bigint,
  outcome       exec_outcome,
  error_class   text,
  error_message text,
  permanent     boolean
) returns table (queue uuid) language plpgsql as $$
#variable_conflict use_variable
declare
  q  uuid;
  pr uuid;
  st job_status;
begin
  update jobs j set
    status = case when permanent or j.attempt_count >= rp.max_attempts
                  then 'dead_letter'::job_status else 'retry_wait'::job_status end,
    run_at = case when permanent or j.attempt_count >= rp.max_attempts then j.run_at
                  else fl.now() + fl.backoff(rp.kind, rp.base_delay_ms, rp.max_delay_ms,
                                             rp.jitter, j.attempt_count) end,
    finished_at = case when permanent or j.attempt_count >= rp.max_attempts then fl.now() end,
    worker_id = null
  from retry_policies rp
  where j.id = job and rp.id = j.retry_policy_id
    and j.status in ('claimed', 'running')
    and (fence is null or j.fence = fence)
  returning j.queue_id, j.project_id, j.status into q, pr, st;

  if not found then
    return;
  end if;

  update job_executions x set
    outcome = outcome, finished_at = fl.now(),
    error_class = error_class, error_message = error_message
  where x.job_id = job and x.finished_at is null;

  delete from job_leases where job_id = job;

  if st = 'dead_letter' then
    insert into dead_letter_jobs
      (job_id, queue_id, project_id, reason,
       last_error_class, last_error_message, execution_history)
    values (job, q, pr,
      case when permanent       then 'permanent_error'
           when outcome = 'lost' then 'lost_exhausted'
           else 'attempts_exhausted' end,
      error_class, error_message,
      coalesce((select jsonb_agg(to_jsonb(e) order by e.attempt)
                  from job_executions e where e.job_id = job), '[]'::jsonb))
    on conflict (job_id) do nothing;
  end if;

  insert into events (topic, entity_id, project_id, payload)
  values (
    case when st = 'dead_letter' then 'job.dead_lettered'
         when outcome = 'lost'   then 'job.lost'
         else 'job.failed' end,
    job, pr,
    jsonb_build_object('status', st::text, 'outcome', outcome::text));

  queue := q;
  return next;
end
$$;
