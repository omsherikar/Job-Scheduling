package worker

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const wakeRetry = time.Second

func listen(ctx context.Context, pool *pgxpool.Pool, wake chan<- struct{}) {
	for ctx.Err() == nil {
		listenOnce(ctx, pool, wake)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wakeRetry):
		}
	}
}

func listenOnce(ctx context.Context, pool *pgxpool.Pool, wake chan<- struct{}) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer func() {
		clean, cancel := context.WithTimeout(context.Background(), wakeRetry)
		conn.Exec(clean, "unlisten fl_events")
		cancel()
		conn.Release()
	}()

	if _, err := conn.Exec(ctx, "listen fl_events"); err != nil {
		return
	}
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}
