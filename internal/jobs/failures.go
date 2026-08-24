package jobs

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const FailureWindow = 24

type Failure struct {
	Class    string
	Count    int
	Variants int
	Sample   string
	First    time.Time
	Last     time.Time
}

type db interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const failuresSQL = `
select coalesce(x.error_class, 'unknown'),
       count(*)::int,
       count(distinct x.error_message)::int,
       coalesce((array_agg(x.error_message order by x.finished_at desc))[1], ''),
       min(x.finished_at), max(x.finished_at)
  from job_executions x
 where x.queue_id = $1
   and x.outcome in ('retryable_error', 'permanent_error', 'timeout', 'lost')
   and x.finished_at >= fl.now() - make_interval(hours => $2)
 group by 1
 order by 2 desc, 1
 limit $3`

func Failures(ctx context.Context, q db, queueID uuid.UUID, hours, limit int) ([]Failure, error) {
	rows, err := q.Query(ctx, failuresSQL, queueID, hours, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Failure{}
	for rows.Next() {
		var f Failure
		if err := rows.Scan(&f.Class, &f.Count, &f.Variants, &f.Sample, &f.First, &f.Last); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
