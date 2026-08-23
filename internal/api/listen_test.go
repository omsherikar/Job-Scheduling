package api

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestListenRetry(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody@127.0.0.1:1/nothing")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	s := &Server{Pool: pool}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		s.listen(ctx)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("listen returned, want still running")
	case <-time.After(500 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("listen still running after cancel, want returned")
	}
}
