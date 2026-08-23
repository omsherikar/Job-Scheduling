package scheduler

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const expiredSQL = `
select j.id
  from jobs j
  join job_leases l on l.job_id = j.id
 where j.status in ('claimed', 'running') and l.expires_at < fl.now()
 order by l.expires_at
 limit $1`

const reportLostSQL = `select queue from fl.report_failure($1, null, 'lost'::exec_outcome, null, null, false)`

const sweepSQL = `
update job_executions set outcome = 'lost', finished_at = fl.now()
 where id in (
   select e.id from job_executions e
     join jobs j on j.id = e.job_id
    where e.finished_at is null
      and j.status not in ('claimed', 'running')
      and e.started_at < fl.now() - make_interval(secs => greatest(j.timeout_ms::double precision / 1000, 60) * 2)
    limit $1
 )`

func Reap(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	rows, err := pool.Query(ctx, expiredSQL, limit)
	if err != nil {
		return 0, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	reaped := 0
	for _, id := range ids {
		var queueID uuid.UUID
		err := pool.QueryRow(ctx, reportLostSQL, id).Scan(&queueID)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return reaped, err
		}
		reaped++
	}
	return reaped, nil
}

func SweepOrphanExecutions(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	tag, err := pool.Exec(ctx, sweepSQL, limit)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
