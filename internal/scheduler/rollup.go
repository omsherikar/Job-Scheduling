package scheduler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const rollupSQL = `
with closed as materialized (
  select queue_id,
         date_trunc('minute', finished_at) as minute,
         percentile_cont(0.5) within group (order by duration_ms) as p50,
         percentile_cont(0.95) within group (order by duration_ms) as p95
    from job_executions
   where finished_at >= fl.now() - make_interval(secs => $1)
     and finished_at < date_trunc('minute', fl.now())
     and duration_ms is not null
   group by queue_id, date_trunc('minute', finished_at)
)
update queue_stats_minute s
   set duration_ms_p50 = closed.p50::int, duration_ms_p95 = closed.p95::int
  from closed
 where s.queue_id = closed.queue_id and s.minute = closed.minute
   and s.duration_ms_p50 is null`

func Rollup(ctx context.Context, pool *pgxpool.Pool, lookback time.Duration) (int, error) {
	tag, err := pool.Exec(ctx, rollupSQL, lookback.Seconds())
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
