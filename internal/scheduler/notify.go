package scheduler

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const eventHeadSQL = `select coalesce(max(id), 0) from events`

const notifySQL = `select pg_notify('fl_events', '')`

func Notify(ctx context.Context, pool *pgxpool.Pool, since int64) (int64, error) {
	var head int64
	if err := pool.QueryRow(ctx, eventHeadSQL).Scan(&head); err != nil {
		return since, err
	}
	if head <= since {
		return since, nil
	}
	if _, err := pool.Exec(ctx, notifySQL); err != nil {
		return since, err
	}
	return head, nil
}
