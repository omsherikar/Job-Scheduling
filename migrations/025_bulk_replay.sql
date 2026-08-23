create function fl.replay_dead(q uuid, cap int, per_sec numeric, actor uuid)
returns table (job uuid) language plpgsql as $$
declare
  room  int;
  ids   uuid[];
  pr    uuid;
begin
  select project_id, case when max_depth is null then cap
         else greatest(max_depth - (select count(*) from jobs
                                     where queue_id = q and status = 'queued'), 0)::int end
    into pr, room
    from queues where id = q;

  if not found then
    return;
  end if;

  room := least(cap, room);
  if room <= 0 then
    return;
  end if;

  select array_agg(d.job_id order by d.dead_at)
    into ids
    from (
      select d.job_id, d.dead_at
        from dead_letter_jobs d
        join jobs j on j.id = d.job_id
       where d.queue_id = q and d.replayed_at is null and j.status = 'dead_letter'
         and not exists (
           select 1 from job_dependencies dep
             join jobs c on c.id = dep.child_id
            where dep.parent_id = d.job_id and c.status = 'cancelled')
       order by d.dead_at
       limit room
    ) d;

  if ids is null then
    return;
  end if;

  update jobs j set
    status = 'queued',
    attempt_count = 0,
    replay_generation = j.replay_generation + 1,
    run_at = fl.now() + make_interval(secs =>
      case when per_sec > 0 then (t.seq - 1)::numeric / per_sec else 0 end),
    finished_at = null,
    worker_id = null
  from (select id, row_number() over (order by id) as seq from unnest(ids) as id) t
  where j.id = t.id and j.status = 'dead_letter';

  update dead_letter_jobs set replayed_at = fl.now(), replayed_by = actor
   where job_id = any(ids);

  perform fl.emit('job.replayed', r.id, pr, jsonb_build_object('bulk', true))
     from (select unnest(ids) as id order by 1) r;

  return query select unnest(ids);
end
$$;
