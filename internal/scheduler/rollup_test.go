package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestRollup(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch.Truncate(time.Minute))

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	for _, secs := range []int{1, 2, 3, 10} {
		jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
		claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
			QueueID: queueID, WorkerID: workerID, FreeSlots: 1, Lease: time.Hour})
		testdb.Must(t, err)
		exec, err := jobs.Start(ctx, pool, jobID, claimed[0].Fence, workerID)
		testdb.Must(t, err)
		testdb.Advance(t, pool, time.Duration(secs)*time.Second)
		testdb.Must(t, jobs.Complete(ctx, pool, jobID, claimed[0].Fence, exec.ID))
		testdb.Advance(t, pool, -time.Duration(secs)*time.Second)
	}

	var p50, p95 *int
	testdb.Must(t, pool.QueryRow(ctx,
		`select duration_ms_p50, duration_ms_p95 from queue_stats_minute where queue_id = $1`,
		queueID).Scan(&p50, &p95))
	if p50 != nil || p95 != nil {
		t.Fatalf("percentiles before rollup = %v, %v, want null, null", p50, p95)
	}

	testdb.Advance(t, pool, 2*time.Minute)
	n, err := scheduler.Rollup(ctx, pool, 10*time.Minute)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("rolled up = %d, want 1", n)
	}

	testdb.Must(t, pool.QueryRow(ctx,
		`select duration_ms_p50, duration_ms_p95 from queue_stats_minute where queue_id = $1`,
		queueID).Scan(&p50, &p95))
	if p50 == nil || *p50 != 2500 {
		t.Errorf("p50 = %v, want 2500", p50)
	}
	if p95 == nil || *p95 != 8950 {
		t.Errorf("p95 = %v, want 8950", p95)
	}
}

func TestRollupOpenMinute(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch.Truncate(time.Minute))

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: queueID, WorkerID: workerID, FreeSlots: 1, Lease: time.Hour})
	testdb.Must(t, err)
	exec, err := jobs.Start(ctx, pool, jobID, claimed[0].Fence, workerID)
	testdb.Must(t, err)
	testdb.Advance(t, pool, time.Second)
	testdb.Must(t, jobs.Complete(ctx, pool, jobID, claimed[0].Fence, exec.ID))

	n, err := scheduler.Rollup(ctx, pool, 10*time.Minute)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("rolled up = %d, want 0", n)
	}
}
