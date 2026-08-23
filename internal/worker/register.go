package worker

import (
	"context"
	"errors"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNoQueue = errors.New("queue not found")

const registerSQL = `
insert into workers (project_id, hostname, pid, max_concurrency)
select q.project_id, $2, $3, $4 from queues q where q.id = $1
returning id`

const subscribeSQL = `
insert into worker_queues (worker_id, queue_id) values ($1, $2)
on conflict do nothing`

const heartbeatSQL = `update workers set last_seen_at = fl.now() where id = $1`

const retireSQL = `update workers set state = $2, last_seen_at = fl.now() where id = $1`

func Register(ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID, concurrency int) (uuid.UUID, error) {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	var id uuid.UUID
	err = pool.QueryRow(ctx, registerSQL, queueID, host, os.Getpid(), concurrency).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrNoQueue
	}
	return id, err
}

func beat(ctx context.Context, pool *pgxpool.Pool, workerID uuid.UUID) error {
	_, err := pool.Exec(ctx, heartbeatSQL, workerID)
	return err
}

func retire(pool *pgxpool.Pool, workerID uuid.UUID, state string) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	pool.Exec(ctx, retireSQL, workerID, state)
}
