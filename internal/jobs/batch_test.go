package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestCompleteBatch(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	var batch []jobs.Done
	for i := 0; i < 5; i++ {
		jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
		c := claimOne(t, ctx, pool, queueID, workerID)
		exec, err := jobs.Start(ctx, pool, jobID, c.Fence, workerID)
		testdb.Must(t, err)
		batch = append(batch, jobs.Done{JobID: jobID, Fence: c.Fence, Exec: exec.ID})
	}
	testdb.Advance(t, pool, 2*time.Second)

	done, err := jobs.CompleteBatch(ctx, pool, batch)
	testdb.Must(t, err)
	if len(done) != 5 {
		t.Fatalf("completed = %d, want 5", len(done))
	}

	for _, d := range batch {
		if s := testdb.JobStatus(t, ctx, pool, d.JobID); s != "completed" {
			t.Errorf("status = %q, want completed", s)
		}
	}

	var m minute
	testdb.Must(t, pool.QueryRow(ctx,
		`select coalesce(sum(completed),0), coalesce(sum(duration_ms_sum),0)
		   from queue_stats_minute where queue_id = $1`, queueID).Scan(&m.completed, &m.durationSum))
	if m.completed != 5 {
		t.Errorf("stats completed = %d, want 5", m.completed)
	}
	if m.durationSum != 10000 {
		t.Errorf("duration sum = %d, want 10000", m.durationSum)
	}

	var inFlight, leases, events int
	testdb.Must(t, pool.QueryRow(ctx, `select in_flight from queues where id = $1`, queueID).Scan(&inFlight))
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from job_leases`).Scan(&leases))
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from events where topic = 'job.completed'`).Scan(&events))
	if inFlight != 0 {
		t.Errorf("in_flight = %d, want 0", inFlight)
	}
	if leases != 0 {
		t.Errorf("leases = %d, want 0", leases)
	}
	if events != 5 {
		t.Errorf("job.completed events = %d, want 5", events)
	}
	testdb.CheckInvariants(t, ctx, pool)
}

func TestCompleteBatchFenced(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
	stale := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	var batch []jobs.Done
	for range 2 {
		c := claimOne(t, ctx, pool, queueID, workerID)
		exec, err := jobs.Start(ctx, pool, c.ID, c.Fence, workerID)
		testdb.Must(t, err)
		fence := c.Fence
		if c.ID == stale {
			fence = c.Fence - 1
		}
		batch = append(batch, jobs.Done{JobID: c.ID, Fence: fence, Exec: exec.ID})
	}

	done, err := jobs.CompleteBatch(ctx, pool, batch)
	testdb.Must(t, err)
	if len(done) != 1 {
		t.Fatalf("completed = %d, want 1", len(done))
	}
	if done[0] == stale {
		t.Errorf("done[0] = %v, want %v", done[0], batch[0].JobID)
	}
	if s := testdb.JobStatus(t, ctx, pool, stale); s != "running" {
		t.Errorf("fenced job status = %q, want running", s)
	}
}
