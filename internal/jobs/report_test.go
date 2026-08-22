package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

var epoch = time.Now().UTC().Truncate(time.Second)

func newJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, queueID, policyID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	must(t, pool.QueryRow(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, run_at)
		values ($1, $2, 'noop', $3, 'queued', fl.now()) returning id`,
		projectID, queueID, policyID).Scan(&id))
	return id
}

func claimOne(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID, workerID uuid.UUID) jobs.Claimed {
	t.Helper()
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID:   queueID,
		WorkerID:  workerID,
		FreeSlots: 1,
		Lease:     30 * time.Second,
	})
	must(t, err)
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	return claimed[0]
}

func promote(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`update jobs set status = 'queued', run_at = fl.now() where id = $1 and status = 'retry_wait'`, jobID)
	must(t, err)
}

func jobStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) string {
	t.Helper()
	var s string
	must(t, pool.QueryRow(ctx, `select status::text from jobs where id = $1`, jobID).Scan(&s))
	return s
}

func setJitterOff(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	name := pool.Config().ConnConfig.Database
	_, err := pool.Exec(context.Background(), fmt.Sprintf("alter database %s set fl.jitter = 'off'", name))
	must(t, err)
	pool.Reset()
}

func TestStaleWorkerFenced(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, epoch)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	workerA := newWorker(t, ctx, pool, projectID)
	a := claimOne(t, ctx, pool, queueID, workerA)
	execA, err := jobs.Start(ctx, pool, jobID, a.Fence, workerA)
	must(t, err)

	testdb.Advance(t, pool, 40*time.Second)
	reaped, err := scheduler.Reap(ctx, pool, 10)
	must(t, err)
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}

	promote(t, ctx, pool, jobID)
	workerB := newWorker(t, ctx, pool, projectID)
	b := claimOne(t, ctx, pool, queueID, workerB)
	if b.Fence != a.Fence+1 {
		t.Fatalf("worker b fence = %d, want %d", b.Fence, a.Fence+1)
	}
	execB, err := jobs.Start(ctx, pool, jobID, b.Fence, workerB)
	must(t, err)
	must(t, jobs.Complete(ctx, pool, jobID, b.Fence, execB.ID))

	err = jobs.Complete(ctx, pool, jobID, a.Fence, execA.ID)
	if !errors.Is(err, jobs.ErrFenced) {
		t.Fatalf("stale complete err = %v, want fenced", err)
	}

	var status string
	var fence int64
	var workerOwned *uuid.UUID
	must(t, pool.QueryRow(ctx,
		`select status::text, fence, worker_id from jobs where id = $1`, jobID).Scan(&status, &fence, &workerOwned))
	if status != "completed" {
		t.Errorf("status = %q, want completed", status)
	}
	if fence != b.Fence {
		t.Errorf("fence = %d, want %d", fence, b.Fence)
	}
	if workerOwned != nil {
		t.Errorf("worker_id = %v, want nil", workerOwned)
	}

	st, err := jobs.Probe(ctx, pool, jobID, a.Fence, workerA)
	must(t, err)
	if st != jobs.StatusFenced {
		t.Errorf("probe = %q, want fenced", st)
	}

	var violations int
	must(t, pool.QueryRow(ctx,
		`select count(*) from job_fence_violations
		  where job_id = $1 and attempted = 'report' and held_fence = $2 and actual_fence = $3`,
		jobID, a.Fence, b.Fence).Scan(&violations))
	if violations == 0 {
		t.Errorf("fence violations = %d, want non-zero", violations)
	}

	checkInvariants(t, ctx, pool)
}

func TestFailRetriesThenDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, epoch)
	setJitterOff(t, pool)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	var maxAttempts int
	must(t, pool.QueryRow(ctx, `select max_attempts from retry_policies where id = $1`, policyID).Scan(&maxAttempts))

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		w := newWorker(t, ctx, pool, projectID)
		c := claimOne(t, ctx, pool, queueID, w)
		exec, err := jobs.Start(ctx, pool, jobID, c.Fence, w)
		must(t, err)

		var count int
		must(t, pool.QueryRow(ctx, `select attempt_count from jobs where id = $1`, jobID).Scan(&count))
		if count != attempt {
			t.Fatalf("attempt_count = %d, want %d", count, attempt)
		}

		must(t, jobs.Fail(ctx, pool, jobID, c.Fence, exec.ID, errors.New("boom")))

		want := "retry_wait"
		if attempt == maxAttempts {
			want = "dead_letter"
		}
		if got := jobStatus(t, ctx, pool, jobID); got != want {
			t.Fatalf("attempt %d status = %q, want %q", attempt, got, want)
		}

		if attempt < maxAttempts {
			var runAt time.Time
			must(t, pool.QueryRow(ctx, `select run_at from jobs where id = $1`, jobID).Scan(&runAt))
			if !runAt.After(epoch) {
				t.Errorf("attempt %d run_at = %v, want after %v", attempt, runAt, epoch)
			}
			promote(t, ctx, pool, jobID)
		}
	}

	var reason string
	var historyLen int
	must(t, pool.QueryRow(ctx,
		`select reason, jsonb_array_length(execution_history) from dead_letter_jobs where job_id = $1`,
		jobID).Scan(&reason, &historyLen))
	if reason != "attempts_exhausted" {
		t.Errorf("reason = %q, want attempts_exhausted", reason)
	}
	if historyLen != maxAttempts {
		t.Errorf("execution_history length = %d, want %d", historyLen, maxAttempts)
	}

	var lost int
	must(t, pool.QueryRow(ctx,
		`select count(*) from job_executions where job_id = $1 and finished_at is null`, jobID).Scan(&lost))
	if lost != 0 {
		t.Errorf("open executions = %d, want 0", lost)
	}

	checkInvariants(t, ctx, pool)
}

func TestExtendLeaseAfterMove(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, epoch)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	w := newWorker(t, ctx, pool, projectID)
	c := claimOne(t, ctx, pool, queueID, w)
	exec, err := jobs.Start(ctx, pool, jobID, c.Fence, w)
	must(t, err)

	ok, err := jobs.ExtendLease(ctx, pool, jobID, c.Fence, 30*time.Second)
	must(t, err)
	if !ok {
		t.Fatal("extend on running job = false, want true")
	}

	must(t, jobs.Complete(ctx, pool, jobID, c.Fence, exec.ID))

	ok, err = jobs.ExtendLease(ctx, pool, jobID, c.Fence, 30*time.Second)
	must(t, err)
	if ok {
		t.Error("extend after completion = true, want false")
	}

	checkInvariants(t, ctx, pool)
}

func TestProbeOutcomes(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		ctx := context.Background()
		pool := testdb.New(t)
		testdb.SetNow(t, pool, epoch)

		projectID, policyID := base(t, ctx, pool)
		queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
		jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

		w := newWorker(t, ctx, pool, projectID)
		c := claimOne(t, ctx, pool, queueID, w)
		_, err := jobs.Start(ctx, pool, jobID, c.Fence, w)
		must(t, err)

		_, err = pool.Exec(ctx,
			`update jobs set status = 'cancelled', finished_at = fl.now(), worker_id = null where id = $1`, jobID)
		must(t, err)
		_, err = pool.Exec(ctx, `delete from job_leases where job_id = $1`, jobID)
		must(t, err)
		_, err = pool.Exec(ctx, `select fl.queue_release($1, 1)`, queueID)
		must(t, err)

		st, err := jobs.Probe(ctx, pool, jobID, c.Fence, w)
		must(t, err)
		if st != jobs.StatusCancelled {
			t.Errorf("probe = %q, want cancelled", st)
		}
		if got := violationCount(t, ctx, pool, jobID); got != 0 {
			t.Errorf("violations = %d, want 0", got)
		}
		checkInvariants(t, ctx, pool)
	})

	t.Run("fenced", func(t *testing.T) {
		ctx := context.Background()
		pool := testdb.New(t)
		testdb.SetNow(t, pool, epoch)

		projectID, policyID := base(t, ctx, pool)
		queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
		jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

		w := newWorker(t, ctx, pool, projectID)
		a := claimOne(t, ctx, pool, queueID, w)
		_, err := jobs.Start(ctx, pool, jobID, a.Fence, w)
		must(t, err)

		testdb.Advance(t, pool, 40*time.Second)
		_, err = scheduler.Reap(ctx, pool, 10)
		must(t, err)
		promote(t, ctx, pool, jobID)
		b := claimOne(t, ctx, pool, queueID, newWorker(t, ctx, pool, projectID))
		if b.Fence == a.Fence {
			t.Fatalf("fence = %d, want > %d", b.Fence, a.Fence)
		}

		st, err := jobs.Probe(ctx, pool, jobID, a.Fence, w)
		must(t, err)
		if st != jobs.StatusFenced {
			t.Errorf("probe = %q, want fenced", st)
		}
		if got := violationCount(t, ctx, pool, jobID); got == 0 {
			t.Errorf("violations = %d, want non-zero", got)
		}
		checkInvariants(t, ctx, pool)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx := context.Background()
		pool := testdb.New(t)
		testdb.SetNow(t, pool, epoch)

		projectID, policyID := base(t, ctx, pool)
		queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
		jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

		w := newWorker(t, ctx, pool, projectID)
		c := claimOne(t, ctx, pool, queueID, w)
		_, err := jobs.Start(ctx, pool, jobID, c.Fence, w)
		must(t, err)

		testdb.Advance(t, pool, 6*time.Minute)
		_, err = scheduler.Reap(ctx, pool, 10)
		must(t, err)

		st, err := jobs.Probe(ctx, pool, jobID, c.Fence, w)
		must(t, err)
		if st != jobs.StatusTimeout {
			t.Errorf("probe = %q, want timeout", st)
		}
		if got := violationCount(t, ctx, pool, jobID); got != 0 {
			t.Errorf("violations = %d, want 0", got)
		}
		checkInvariants(t, ctx, pool)
	})
}

func violationCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) int {
	t.Helper()
	var n int
	must(t, pool.QueryRow(ctx, `select count(*) from job_fence_violations where job_id = $1`, jobID).Scan(&n))
	return n
}
