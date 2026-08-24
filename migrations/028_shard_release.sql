create or replace function fl.report_success(job uuid, fence bigint, exec uuid)
returns table (queue uuid) language plpgsql as $$
#variable_conflict use_variable
declare
  q    uuid;
  pr   uuid;
  sh   smallint;
  dur  bigint;
  woke uuid[];
begin
  update jobs j set status = 'completed', finished_at = fl.now(), worker_id = null
   where j.id = job and j.fence = fence and j.status = 'running'
  returning j.queue_id, j.project_id, j.shard into q, pr, sh;

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

  perform fl.queue_release(q, sh, 1);

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
  sh     smallint;
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
  returning j.queue_id, j.project_id, j.status, j.shard into q, pr, st, sh;

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

  perform fl.queue_release(q, sh, 1);

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

create or replace function fl.report_snooze(job uuid, fence bigint, exec uuid, delay_ms bigint)
returns table (queue uuid) language plpgsql as $$
#variable_conflict use_variable
declare
  q  uuid;
  pr uuid;
  sh smallint;
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
  returning j.queue_id, j.project_id, j.shard into q, pr, sh;

  if not found then
    return;
  end if;

  update job_executions x set outcome = 'snoozed', finished_at = fl.now()
   where x.id = exec and x.finished_at is null;

  delete from job_leases where job_id = job;

  perform fl.queue_release(q, sh, 1);

  perform fl.emit('job.snoozed', job, pr, jsonb_build_object('delay_ms', delay_ms));

  queue := q;
  return next;
end
$$;

create or replace function fl.report_success_batch(ids uuid[], fences bigint[], execs uuid[])
returns table (job uuid) language plpgsql as $$
declare
  done_ids   uuid[];
  done_execs uuid[];
  woke       uuid[];
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

  select array_agg(d.id) into woke
    from (select unnest(done_ids) as j order by 1) t,
         lateral fl.finish_dependents(t.j) d;

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

  perform fl.queue_release(t.queue_id, t.shard, t.n)
     from (select queue_id, shard, count(*)::int as n from jobs
            where id = any(done_ids) group by queue_id, shard
            order by queue_id, shard) t;

  perform fl.emit('job.queued', w.id, w.project_id, '{}'::jsonb)
     from (select id, project_id from jobs
            where id = any(woke) and status = 'queued'
            order by project_id, id) w;

  perform fl.emit('job.completed', j.id, j.project_id,
                  jsonb_build_object('status', 'completed', 'outcome', 'success'))
     from (select id, project_id from jobs
            where id = any(done_ids) order by project_id, id) j;

  return query select unnest(done_ids);
end
$$;
