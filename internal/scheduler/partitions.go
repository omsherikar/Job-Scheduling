package scheduler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const partitionLockSQL = `select pg_try_advisory_xact_lock(hashtext('fl_partitions'))`

const ensurePartitionsSQL = `select fl.ensure_partitions($1, (fl.now())::date, (fl.now() + interval '7 days')::date)`

const dropPartitionsSQL = `select fl.drop_partitions_before($1, (fl.now() - make_interval(secs => $2))::date)`

const lowWaterSQL = `
update events_retention set low_water_id = greatest(low_water_id, coalesce(
  (select min(id) from events),
  coalesce(pg_sequence_last_value(pg_get_serial_sequence('events', 'id')::regclass), 0) + 1))`

var partitionParents = []string{
	"events",
	"job_logs",
	"jobs_archive",
	"job_executions_archive",
	"job_logs_archive",
}

func Maintain(ctx context.Context, pool *pgxpool.Pool, retention time.Duration) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var got bool
	if err := tx.QueryRow(ctx, partitionLockSQL).Scan(&got); err != nil {
		return 0, err
	}
	if !got {
		return 0, nil
	}

	dropped := 0
	for _, parent := range partitionParents {
		if _, err := tx.Exec(ctx, ensurePartitionsSQL, parent); err != nil {
			return 0, err
		}
		var n int
		if err := tx.QueryRow(ctx, dropPartitionsSQL, parent, retention.Seconds()).Scan(&n); err != nil {
			return 0, err
		}
		dropped += n
	}

	if _, err := tx.Exec(ctx, lowWaterSQL); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return dropped, nil
}
