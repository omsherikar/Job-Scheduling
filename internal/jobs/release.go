package jobs

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const releaseSQL = `select fl.queue_release($1, $2, $3)`

func Release(ctx context.Context, tx pgx.Tx, queueID uuid.UUID, shard int16, n int) error {
	_, err := tx.Exec(ctx, releaseSQL, queueID, shard, n)
	return err
}

func ReconcileInFlight(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var n int
	err := pool.QueryRow(ctx, "select fl.reconcile_in_flight()").Scan(&n)
	return n, err
}
