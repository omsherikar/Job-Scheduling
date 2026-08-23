create or replace function fl.report_success(job uuid, fence bigint, exec uuid)
returns table (queue uuid) language plpgsql as $$
#variable_conflict use_variable
declare
  q    uuid;
  pr   uuid;
  dur  bigint;
  woke uuid[];
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

  select array_agg(d.id) into woke from fl.finish_dependents(job) d;

  delete from job_leases where job_id = job;

  insert into queue_stats_minute (queue_id, minute, completed, duration_ms_sum)
  values (q, date_trunc('minute', fl.now()), 1, coalesce(dur, 0))
  on conflict (queue_id, minute) do update set
    completed       = queue_stats_minute.completed + excluded.completed,
    duration_ms_sum = queue_stats_minute.duration_ms_sum + excluded.duration_ms_sum;

  perform fl.queue_release(q, 1);

  perform fl.emit('job.queued', w.id, w.project_id, '{}'::jsonb)
     from (select id, project_id from jobs
            where id = any(woke) and status = 'queued'
            order by project_id, id) w;

  perform fl.emit('job.completed', job, pr,
    jsonb_build_object('status', 'completed', 'outcome', 'success'));

  queue := q;
  return next;
end
$$;

create or replace function fl.report_failure(
  job           uuid,
  fence         bigint,
  outcome       exec_outcome,
  error_class   text,
  error_message text,
  permanent     boolean
) returns table (queue uuid) language plpgsql as $$
#variable_conflict use_variable
declare
  q      uuid;
  pr     uuid;
  st     job_status;
  killed uuid[];
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

  if st = 'dead_letter' then
    select array_agg(d.id) into killed from fl.cancel_descendants(job) d;
  end if;

  update job_executions x set
    outcome = outcome, finished_at = fl.now(),
    error_class = error_class, error_message = error_message
  where x.job_id = job and x.finished_at is null;

  delete from job_leases where job_id = job;

  insert into queue_stats_minute (queue_id, minute, failed, dead_lettered)
  values (q, date_trunc('minute', fl.now()),
          case when st = 'retry_wait'  then 1 else 0 end,
          case when st = 'dead_letter' then 1 else 0 end)
  on conflict (queue_id, minute) do update set
    failed        = queue_stats_minute.failed + excluded.failed,
    dead_lettered = queue_stats_minute.dead_lettered + excluded.dead_lettered;

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

  perform fl.queue_release(q, 1);

  perform fl.emit('job.cancelled', k.id, k.project_id,
                  jsonb_build_object('from', 'ancestor_dead_lettered'))
     from (select id, project_id from jobs
            where id = any(killed) order by project_id, id) k;

  perform fl.emit(
    case when st = 'dead_letter' then 'job.dead_lettered'
         when outcome = 'lost'   then 'job.lost'
         else 'job.failed' end,
    job, pr,
    jsonb_build_object('status', st::text, 'outcome', outcome::text));

  queue := q;
  return next;
end
$$;
