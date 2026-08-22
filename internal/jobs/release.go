package jobs

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Release(ctx context.Context, tx pgx.Tx, queueID uuid.UUID, n int) error {
	_, err := tx.Exec(ctx, "select fl.queue_release($1, $2)", queueID, n)
	return err
}

func ReconcileInFlight(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var n int
	err := pool.QueryRow(ctx, "select fl.reconcile_in_flight()").Scan(&n)
	return n, err
}
