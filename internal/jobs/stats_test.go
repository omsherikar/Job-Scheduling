package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

type minute struct {
	completed    int
	failed       int
	deadLettered int
	durationSum  int64
}

func stats(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID) minute {
	t.Helper()
	var m minute
	testdb.Must(t, pool.QueryRow(ctx,
		`select coalesce(sum(completed), 0), coalesce(sum(failed), 0),
		        coalesce(sum(dead_lettered), 0), coalesce(sum(duration_ms_sum), 0)
		   from queue_stats_minute where queue_id = $1`, queueID).
		Scan(&m.completed, &m.failed, &m.deadLettered, &m.durationSum))
	return m
}

func TestCompleteStats(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	for i := 0; i < 2; i++ {
		jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
		c := claimOne(t, ctx, pool, queueID, workerID)
		exec, err := jobs.Start(ctx, pool, jobID, c.Fence, workerID)
		testdb.Must(t, err)
		testdb.Advance(t, pool, 3*time.Second)
		testdb.Must(t, jobs.Complete(ctx, pool, jobID, c.Fence, exec.ID))
	}

	m := stats(t, ctx, pool, queueID)
	if m.completed != 2 {
		t.Errorf("completed = %d, want 2", m.completed)
	}
	if m.durationSum != 6000 {
		t.Errorf("duration sum = %d, want 6000", m.durationSum)
	}
	if m.failed != 0 || m.deadLettered != 0 {
		t.Errorf("failed, dead lettered = %d, %d, want 0, 0", m.failed, m.deadLettered)
	}
}

func TestFailStats(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	c := claimOne(t, ctx, pool, queueID, workerID)
	_, err := jobs.Start(ctx, pool, jobID, c.Fence, workerID)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Fail(ctx, pool, jobID, c.Fence, errors.New("boom")))

	m := stats(t, ctx, pool, queueID)
	if m.failed != 1 {
		t.Errorf("failed = %d, want 1", m.failed)
	}
	if m.deadLettered != 0 {
		t.Errorf("dead lettered = %d, want 0", m.deadLettered)
	}

	promote(t, ctx, pool, jobID)
	c = claimOne(t, ctx, pool, queueID, workerID)
	_, err = jobs.Start(ctx, pool, jobID, c.Fence, workerID)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Fail(ctx, pool, jobID, c.Fence, jobs.ErrPermanent))

	m = stats(t, ctx, pool, queueID)
	if m.deadLettered != 1 {
		t.Errorf("dead lettered = %d, want 1", m.deadLettered)
	}
	if m.completed != 0 {
		t.Errorf("completed = %d, want 0", m.completed)
	}
}

func TestTerminalEvents(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	good := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
	c := claimOne(t, ctx, pool, queueID, workerID)
	exec, err := jobs.Start(ctx, pool, good, c.Fence, workerID)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Complete(ctx, pool, good, c.Fence, exec.ID))

	bad := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
	c = claimOne(t, ctx, pool, queueID, workerID)
	_, err = jobs.Start(ctx, pool, bad, c.Fence, workerID)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Fail(ctx, pool, bad, c.Fence, jobs.ErrPermanent))

	for _, tc := range []struct {
		jobID uuid.UUID
		topic string
	}{
		{good, "job.completed"},
		{bad, "job.dead_lettered"},
	} {
		var n int
		testdb.Must(t, pool.QueryRow(ctx,
			`select count(*) from events where topic = $1 and entity_id = $2 and project_id = $3`,
			tc.topic, tc.jobID, projectID).Scan(&n))
		if n != 1 {
			t.Errorf("%s events = %d, want 1", tc.topic, n)
		}
	}
}
