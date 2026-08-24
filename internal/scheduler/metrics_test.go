package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestMetricViews(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	_, err := pool.Exec(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, priority, run_at)
		values ($1, $2, 'noop', $3, 'queued', 3, fl.now())`, projectID, queueID, policyID)
	testdb.Must(t, err)
	testdb.Advance(t, pool, 5*time.Minute)
	_, err = pool.Exec(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, priority, run_at)
		values ($1, $2, 'noop', $3, 'queued', 0, fl.now())`, projectID, queueID, policyID)
	testdb.Must(t, err)

	var tiers int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from fl.queue_age where queue_id = $1`, queueID).Scan(&tiers))
	if tiers != 2 {
		t.Fatalf("tiers = %d, want 2", tiers)
	}
	var age int
	testdb.Must(t, pool.QueryRow(ctx,
		`select oldest_ready_seconds from fl.queue_age where queue_id = $1 and priority = 3`, queueID).Scan(&age))
	if age != 300 {
		t.Errorf("priority 3 age = %d, want 300", age)
	}

	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: queueID, WorkerID: workerID, FreeSlots: 1, Lease: time.Minute})
	testdb.Must(t, err)
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}

	var lag, overdue int
	testdb.Must(t, pool.QueryRow(ctx, `select seconds, overdue from fl.reaper_lag`).Scan(&lag, &overdue))
	if overdue != 0 {
		t.Errorf("overdue leases = %d, want 0", overdue)
	}

	testdb.Advance(t, pool, 5*time.Minute)
	testdb.Must(t, pool.QueryRow(ctx, `select seconds, overdue from fl.reaper_lag`).Scan(&lag, &overdue))
	if overdue != 1 {
		t.Fatalf("overdue leases = %d, want 1", overdue)
	}
	if lag < 200 {
		t.Errorf("reaper lag = %ds, want at least 200", lag)
	}

	var fenced int
	testdb.Must(t, pool.QueryRow(ctx, `select total from fl.fenced_writes`).Scan(&fenced))
	if fenced != 0 {
		t.Errorf("fenced writes = %d, want 0", fenced)
	}

	_, err = pool.Exec(ctx, `insert into job_fence_violations (job_id, worker_id, attempted, held_fence)
		values ($1, $2, 'report', 1)`, claimed[0].ID, workerID)
	testdb.Must(t, err)
	testdb.Must(t, pool.QueryRow(ctx, `select total from fl.fenced_writes`).Scan(&fenced))
	if fenced != 1 {
		t.Errorf("fenced writes = %d, want 1", fenced)
	}
}
