package db_test

import (
	"context"
	"testing"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestMigrate(t *testing.T) {
	pool := testdb.New(t)

	var n int
	err := pool.QueryRow(context.Background(),
		"select count(*) from pg_tables where schemaname = 'public'").Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n < 14 {
		t.Fatalf("tables = %d, want at least 14", n)
	}
}

func TestFSMRejectsQueuedToCompleted(t *testing.T) {
	pool := testdb.New(t)

	var legal bool
	err := pool.QueryRow(context.Background(), `select exists (
		select 1 from job_transitions where from_status = 'queued' and to_status = 'completed')`).Scan(&legal)
	if err != nil {
		t.Fatal(err)
	}
	if legal {
		t.Fatal("queued -> completed is in job_transitions")
	}
}

func TestBackoff(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, "set fl.jitter = 'off'"); err != nil {
		t.Fatal(err)
	}

	var first, capped float64
	err := pool.QueryRow(ctx,
		`select extract(epoch from fl.backoff('exponential', 1000, 60000, true, 1)),
		        extract(epoch from fl.backoff('exponential', 1000, 60000, true, 20))`).Scan(&first, &capped)
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("first = %v, want 1", first)
	}
	if capped != 60 {
		t.Fatalf("capped = %v, want 60", capped)
	}
}
