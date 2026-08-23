package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/db"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestTransactionTimeout(t *testing.T) {
	ctx := context.Background()
	clone := testdb.New(t)

	pool, err := db.Open(ctx, clone.Config().ConnConfig.ConnString(), 4, time.Second)
	testdb.Must(t, err)
	defer pool.Close()

	var setting string
	testdb.Must(t, pool.QueryRow(ctx, `show transaction_timeout`).Scan(&setting))
	if setting != "1s" {
		t.Fatalf("transaction_timeout = %q, want 1s", setting)
	}

	tx, err := pool.Begin(ctx)
	testdb.Must(t, err)
	var one int
	testdb.Must(t, tx.QueryRow(ctx, `select 1`).Scan(&one))

	time.Sleep(1500 * time.Millisecond)

	if err := tx.QueryRow(ctx, `select 1`).Scan(&one); err == nil {
		t.Error("err = nil, want non-nil")
	}
	tx.Rollback(ctx)

	if err := pool.QueryRow(ctx, `select 1`).Scan(&one); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if one != 1 {
		t.Errorf("select 1 = %d, want 1", one)
	}
}

func TestNoTransactionTimeoutByDefault(t *testing.T) {
	ctx := context.Background()
	clone := testdb.New(t)

	pool, err := db.Open(ctx, clone.Config().ConnConfig.ConnString(), 2, 0)
	testdb.Must(t, err)
	defer pool.Close()

	var setting string
	testdb.Must(t, pool.QueryRow(ctx, `show transaction_timeout`).Scan(&setting))
	if setting != "0" {
		t.Errorf("transaction_timeout = %q, want 0", setting)
	}
}
