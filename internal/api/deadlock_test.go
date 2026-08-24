package api_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func deadlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx,
		`select deadlocks from pg_stat_database where datname = current_database()`).Scan(&n))
	return n
}

func running(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ten tenant, n int) []jobs.Claimed {
	t.Helper()
	worker := testdb.NewWorker(t, ctx, pool, ten.projectID)
	for i := 0; i < n; i++ {
		testdb.NewJob(t, ctx, pool, ten.projectID, ten.queueID, ten.policyID)
	}
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: ten.queueID, WorkerID: worker, FreeSlots: n, Lease: time.Hour})
	testdb.Must(t, err)
	if len(claimed) != n {
		t.Fatalf("claimed = %d, want %d", len(claimed), n)
	}
	for _, c := range claimed {
		if _, err := jobs.Start(ctx, pool, c.ID, c.Fence, worker); err != nil {
			t.Fatal(err)
		}
	}
	return claimed
}

func TestCancelAgainstFail(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	_, err := pool.Exec(ctx, `update queues set max_concurrency = 100 where id = $1`, ten.queueID)
	testdb.Must(t, err)

	claimed := running(t, ctx, pool, ten, 40)
	before := deadlocks(t, ctx, pool)

	var wg sync.WaitGroup
	codes := make([]int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, _, _ := do(t, base, token, "POST", "/jobs/"+claimed[i].ID.String()+"/cancel", nil, nil)
			codes[i] = code
		}(i)
	}
	fails := make([]error, 20)
	for i := 20; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fails[i-20] = jobs.Fail(ctx, pool, claimed[i].ID, claimed[i].Fence, errors.New("boom"))
		}(i)
	}
	wg.Wait()

	if got := deadlocks(t, ctx, pool) - before; got != 0 {
		t.Errorf("deadlocks = %d, want 0", got)
	}
	for i, code := range codes {
		if code != http.StatusOK {
			t.Errorf("cancel %d status = %d, want 200", i, code)
		}
	}
	for i, err := range fails {
		if err != nil {
			t.Errorf("fail %d err = %v, want nil", i, err)
		}
	}

	var inFlight int
	testdb.Must(t, pool.QueryRow(ctx,
		`select fl.in_flight($1)`, ten.queueID).Scan(&inFlight))
	if inFlight != 0 {
		t.Errorf("in_flight = %d, want 0", inFlight)
	}
	testdb.CheckInvariants(t, ctx, pool)
}
