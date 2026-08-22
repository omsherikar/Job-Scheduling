package scheduler

import (
	"context"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

const expiredSQL = `
select j.id
  from jobs j
  join job_leases l on l.job_id = j.id
 where j.status in ('claimed', 'running') and l.expires_at < fl.now()
 order by l.expires_at
 for update of j skip locked
 limit $1`

const reportLostSQL = `select queue from fl.report_failure($1, null, 'lost'::exec_outcome, null, null, false)`

const sweepSQL = `
update job_executions e set outcome = 'lost', finished_at = fl.now()
from jobs j
where j.id = e.job_id
  and e.finished_at is null
  and j.status not in ('claimed', 'running')
  and e.started_at < fl.now() - make_interval(secs => greatest(j.timeout_ms::double precision / 1000, 60) * 2)`

func Reap(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, expiredSQL, limit)
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
	if len(ids) == 0 {
		return 0, nil
	}

	freed := map[uuid.UUID]int{}
	for _, id := range ids {
		var queueID uuid.UUID
		if err := tx.QueryRow(ctx, reportLostSQL, id).Scan(&queueID); err != nil {
			return 0, err
		}
		freed[queueID]++
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
	return len(ids), nil
}

func SweepOrphanExecutions(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	tag, err := pool.Exec(ctx, sweepSQL)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
