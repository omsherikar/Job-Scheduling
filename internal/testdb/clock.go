package testdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func SetNow(t *testing.T, pool *pgxpool.Pool, at time.Time) {
	t.Helper()
	name := pool.Config().ConnConfig.Database
	ctx := context.Background()

	stmts := []string{
		fmt.Sprintf("alter database %s set fl.testing = 'on'", name),
		fmt.Sprintf("alter database %s set fl.now = '%s'", name, at.UTC().Format(time.RFC3339Nano)),
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf("set clock: %v", err)
		}
	}
	pool.Reset()
}

func Advance(t *testing.T, pool *pgxpool.Pool, d time.Duration) time.Time {
	t.Helper()
	var at time.Time
	if err := pool.QueryRow(context.Background(), "select fl.now()").Scan(&at); err != nil {
		t.Fatalf("read clock: %v", err)
	}
	next := at.Add(d)
	SetNow(t, pool, next)
	return next
}

func ResetNow(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	name := pool.Config().ConnConfig.Database
	ctx := context.Background()
	for _, s := range []string{
		fmt.Sprintf("alter database %s reset fl.now", name),
		fmt.Sprintf("alter database %s reset fl.testing", name),
	} {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf("reset clock: %v", err)
		}
	}
	pool.Reset()
}
