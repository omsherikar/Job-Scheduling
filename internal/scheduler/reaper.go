package scheduler

import (
	"context"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

const reapSQL = `
with expired as (
  select j.id
    from jobs j
    join job_leases l on l.job_id = j.id
   where j.status in ('claimed', 'running') and l.expires_at < fl.now()
   order by l.expires_at
   for update of j skip locked
   limit $1
),
reaped as (
  update jobs j set
    status = case when j.attempt_count >= rp.max_attempts
                  then 'dead_letter'::job_status else 'retry_wait'::job_status end,
    run_at = case when j.attempt_count >= rp.max_attempts then j.run_at
                  else fl.now() + fl.backoff(rp.kind, rp.base_delay_ms, rp.max_delay_ms, rp.jitter, j.attempt_count) end,
    finished_at = case when j.attempt_count >= rp.max_attempts then fl.now() else j.finished_at end,
    worker_id = null
  from expired e, retry_policies rp
  where j.id = e.id and rp.id = j.retry_policy_id
  returning j.id, j.queue_id, j.project_id, j.status
),
closed as (
  update job_executions x set outcome = 'lost', finished_at = fl.now()
  from reaped r
  where x.job_id = r.id and x.finished_at is null
  returning x.job_id
),
dropped as (
  delete from job_leases l using reaped r where l.job_id = r.id
  returning l.job_id
),
ev as (
  insert into events (topic, entity_id, project_id, payload)
  select case when r.status = 'dead_letter' then 'job.dead_lettered' else 'job.lost' end,
         r.id, r.project_id,
         jsonb_build_object('status', r.status::text, 'outcome', 'lost')
    from reaped r
  returning 1
)
select id, queue_id, project_id, status from reaped`

const reapDeadLetterSQL = `
insert into dead_letter_jobs
  (job_id, queue_id, project_id, reason, last_error_class, last_error_message, execution_history)
select $1, $2, $3, 'lost_exhausted',
  (select e.error_class from job_executions e where e.job_id = $1 order by e.attempt desc limit 1),
  (select e.error_message from job_executions e where e.job_id = $1 order by e.attempt desc limit 1),
  coalesce((select jsonb_agg(to_jsonb(e) order by e.attempt)
              from job_executions e where e.job_id = $1), '[]'::jsonb)
on conflict (job_id) do nothing`

const sweepSQL = `
update job_executions e set outcome = 'lost', finished_at = fl.now()
from jobs j
where j.id = e.job_id
  and e.finished_at is null
  and j.status not in ('claimed', 'running')
  and e.started_at < fl.now() - make_interval(secs => greatest(j.timeout_ms::double precision / 1000, 60) * 2)`

type reaped struct {
	id        uuid.UUID
	queueID   uuid.UUID
	projectID uuid.UUID
	status    string
}

func Reap(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, reapSQL, limit)
	if err != nil {
		return 0, err
	}
	var out []reaped
	for rows.Next() {
		var r reaped
		if err := rows.Scan(&r.id, &r.queueID, &r.projectID, &r.status); err != nil {
			rows.Close()
			return 0, err
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(out) == 0 {
		return 0, nil
	}

	for _, r := range out {
		if r.status != "dead_letter" {
			continue
		}
		if _, err := tx.Exec(ctx, reapDeadLetterSQL, r.id, r.queueID, r.projectID); err != nil {
			return 0, err
		}
	}

	freed := map[uuid.UUID]int{}
	for _, r := range out {
		freed[r.queueID]++
	}
	queueIDs := make([]uuid.UUID, 0, len(freed))
	for q := range freed {
		queueIDs = append(queueIDs, q)
	}
	sort.Slice(queueIDs, func(i, j int) bool { return queueIDs[i].String() < queueIDs[j].String() })
	for _, q := range queueIDs {
		if err := jobs.Release(ctx, tx, q, freed[q]); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(out), nil
}

func SweepOrphanExecutions(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	tag, err := pool.Exec(ctx, sweepSQL)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
