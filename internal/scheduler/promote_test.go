package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func priority(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) int {
	t.Helper()
	var p int
	testdb.Must(t, pool.QueryRow(ctx, `select priority from jobs where id = $1`, jobID).Scan(&p))
	return p
}

func TestPromoteScheduled(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewScheduledJob(t, ctx, pool, projectID, queueID, policyID, time.Hour)

	testdb.Advance(t, pool, 2*time.Hour)
	n, err := scheduler.Promote(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("promoted = %d, want 1", n)
	}
	if got := testdb.JobStatus(t, ctx, pool, jobID); got != "queued" {
		t.Errorf("status = %q, want queued", got)
	}

	var events int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from events where topic = 'job.queued' and entity_id = $1`, jobID).Scan(&events))
	if events != 1 {
		t.Errorf("job.queued events = %d, want 1", events)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestPromoteRetryWait(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewRetryWaitJob(t, ctx, pool, projectID, queueID, policyID, time.Hour)

	testdb.Advance(t, pool, 2*time.Hour)
	n, err := scheduler.Promote(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("promoted = %d, want 1", n)
	}
	if got := testdb.JobStatus(t, ctx, pool, jobID); got != "queued" {
		t.Errorf("status = %q, want queued", got)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestPromoteBlockedByDeps(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)

	var jobID uuid.UUID
	testdb.Must(t, pool.QueryRow(ctx, `insert into jobs
		(project_id, queue_id, type, retry_policy_id, status, run_at, pending_deps)
		values ($1, $2, 'noop', $3, 'scheduled', fl.now(), 1) returning id`,
		projectID, queueID, policyID).Scan(&jobID))

	n, err := scheduler.Promote(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("promoted = %d, want 0", n)
	}
	if got := testdb.JobStatus(t, ctx, pool, jobID); got != "scheduled" {
		t.Errorf("status = %q, want scheduled", got)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestPromoteFuture(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewScheduledJob(t, ctx, pool, projectID, queueID, policyID, time.Hour)

	n, err := scheduler.Promote(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("promoted = %d, want 0", n)
	}
	if got := testdb.JobStatus(t, ctx, pool, jobID); got != "scheduled" {
		t.Errorf("status = %q, want scheduled", got)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestAgeCeiling(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	testdb.Advance(t, pool, 6*time.Minute)

	for want := 1; want <= 3; want++ {
		n, err := scheduler.Age(ctx, pool, 5*time.Minute, 3, 100)
		testdb.Must(t, err)
		if n != 1 {
			t.Fatalf("aged = %d, want 1", n)
		}
		if got := priority(t, ctx, pool, jobID); got != want {
			t.Fatalf("priority = %d, want %d", got, want)
		}
	}

	n, err := scheduler.Age(ctx, pool, 5*time.Minute, 3, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("aged = %d, want 0", n)
	}
	if got := priority(t, ctx, pool, jobID); got != 3 {
		t.Fatalf("priority = %d, want 3", got)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
