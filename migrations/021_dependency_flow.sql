create function fl.cancel_descendants(root uuid)
returns table (id uuid, project_id uuid) language sql as $$
  with recursive down as (
    select d.child_id as id, array[d.parent_id, d.child_id] as path
      from job_dependencies d
     where d.parent_id = root
    union all
    select d.child_id, down.path || d.child_id
      from job_dependencies d
      join down on d.parent_id = down.id
     where not d.child_id = any(down.path)
  )
  update jobs j set
    status = 'cancelled', finished_at = fl.now(), pending_deps = 0, worker_id = null
   where j.id in (select down.id from down)
     and j.status in ('scheduled', 'queued', 'retry_wait')
  returning j.id, j.project_id
$$;

create function fl.finish_dependents(parent uuid)
returns table (id uuid, project_id uuid) language sql as $$
  update jobs c set
    pending_deps = c.pending_deps - 1,
    status = case when c.pending_deps - 1 = 0 and c.run_at <= fl.now()
                  then 'queued'::job_status else c.status end
   where c.id in (select child_id from job_dependencies where parent_id = parent)
     and c.status = 'scheduled'
     and c.pending_deps > 0
  returning c.id, c.project_id
$$;
