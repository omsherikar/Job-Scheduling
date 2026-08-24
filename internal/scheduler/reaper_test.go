package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func claimOne(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID, workerID uuid.UUID) jobs.Claimed {
	t.Helper()
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID:   queueID,
		WorkerID:  workerID,
		FreeSlots: 1,
		Lease:     30 * time.Second,
	})
	testdb.Must(t, err)
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	return claimed[0]
}

func promote(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`update jobs set status = 'queued', run_at = fl.now() where id = $1 and status = 'retry_wait'`, jobID)
	testdb.Must(t, err)
}

func TestReaperRecoversCrash(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	workerA := testdb.NewWorker(t, ctx, pool, projectID)
	a := claimOne(t, ctx, pool, queueID, workerA)
	execA, err := jobs.Start(ctx, pool, jobID, a.Fence, workerA)
	testdb.Must(t, err)

	testdb.Advance(t, pool, 40*time.Second)
	reaped, err := scheduler.Reap(ctx, pool, 10)
	testdb.Must(t, err)
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}

	var status string
	var attemptCount int
	var inFlight int
	var leases int
	testdb.Must(t, pool.QueryRow(ctx,
		`select status::text, attempt_count from jobs where id = $1`, jobID).Scan(&status, &attemptCount))
	testdb.Must(t, pool.QueryRow(ctx, `select fl.in_flight($1)`, queueID).Scan(&inFlight))
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from job_leases where job_id = $1`, jobID).Scan(&leases))

	if status != "retry_wait" {
		t.Errorf("status = %q, want retry_wait", status)
	}
	if attemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1", attemptCount)
	}
	if inFlight != 0 {
		t.Errorf("in_flight = %d, want 0", inFlight)
	}
	if leases != 0 {
		t.Errorf("leases = %d, want 0", leases)
	}

	var outcome string
	var finished bool
	testdb.Must(t, pool.QueryRow(ctx,
		`select outcome::text, finished_at is not null from job_executions where id = $1`,
		execA.ID).Scan(&outcome, &finished))
	if outcome != "lost" {
		t.Errorf("execution outcome = %q, want lost", outcome)
	}
	if !finished {
		t.Error("execution finished_at is null, want set")
	}

	promote(t, ctx, pool, jobID)
	workerB := testdb.NewWorker(t, ctx, pool, projectID)
	b := claimOne(t, ctx, pool, queueID, workerB)
	if b.Fence <= a.Fence {
		t.Fatalf("reclaim fence = %d, want > %d", b.Fence, a.Fence)
	}
	execB, err := jobs.Start(ctx, pool, jobID, b.Fence, workerB)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Complete(ctx, pool, jobID, b.Fence, execB.ID))

	if got := testdb.JobStatus(t, ctx, pool, jobID); got != "completed" {
		t.Errorf("final status = %q, want completed", got)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestReaperDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_attempts = 1 where id = $1`, policyID)
	testdb.Must(t, err)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	w := testdb.NewWorker(t, ctx, pool, projectID)
	c := claimOne(t, ctx, pool, queueID, w)
	execID, err := jobs.Start(ctx, pool, jobID, c.Fence, w)
	testdb.Must(t, err)

	testdb.Advance(t, pool, 40*time.Second)
	reaped, err := scheduler.Reap(ctx, pool, 10)
	testdb.Must(t, err)
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}

	if got := testdb.JobStatus(t, ctx, pool, jobID); got != "dead_letter" {
		t.Errorf("status = %q, want dead_letter", got)
	}

	var reason string
	var historyLen int
	testdb.Must(t, pool.QueryRow(ctx,
		`select reason, jsonb_array_length(execution_history) from dead_letter_jobs where job_id = $1`,
		jobID).Scan(&reason, &historyLen))
	if reason != "lost_exhausted" {
		t.Errorf("reason = %q, want lost_exhausted", reason)
	}
	if historyLen != 1 {
		t.Errorf("execution_history length = %d, want 1", historyLen)
	}

	var outcome string
	testdb.Must(t, pool.QueryRow(ctx,
		`select outcome::text from job_executions where id = $1`, execID.ID).Scan(&outcome))
	if outcome != "lost" {
		t.Errorf("execution outcome = %q, want lost", outcome)
	}

	var inFlight int
	testdb.Must(t, pool.QueryRow(ctx, `select fl.in_flight($1)`, queueID).Scan(&inFlight))
	if inFlight != 0 {
		t.Errorf("in_flight = %d, want 0", inFlight)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestSweepGrace(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	w := testdb.NewWorker(t, ctx, pool, projectID)
	c := claimOne(t, ctx, pool, queueID, w)
	_, err := jobs.Start(ctx, pool, jobID, c.Fence, w)
	testdb.Must(t, err)

	_, err = pool.Exec(ctx,
		`update jobs set status = 'completed', finished_at = fl.now(), worker_id = null where id = $1`, jobID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `delete from job_leases where job_id = $1`, jobID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `select fl.queue_release($1, 0::smallint, 1)`, queueID)
	testdb.Must(t, err)

	swept, err := scheduler.SweepOrphanExecutions(ctx, pool, 100)
	testdb.Must(t, err)
	if swept != 0 {
		t.Fatalf("swept within grace = %d, want 0", swept)
	}
	var open int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from job_executions where job_id = $1 and finished_at is null`, jobID).Scan(&open))
	if open != 1 {
		t.Fatalf("open executions within grace = %d, want 1", open)
	}

	testdb.Advance(t, pool, 11*time.Minute)
	swept, err = scheduler.SweepOrphanExecutions(ctx, pool, 100)
	testdb.Must(t, err)
	if swept != 1 {
		t.Fatalf("swept past grace = %d, want 1", swept)
	}

	var outcome string
	var finished bool
	testdb.Must(t, pool.QueryRow(ctx,
		`select outcome::text, finished_at is not null from job_executions where job_id = $1`,
		jobID).Scan(&outcome, &finished))
	if outcome != "lost" {
		t.Errorf("execution outcome = %q, want lost", outcome)
	}
	if !finished {
		t.Error("execution finished_at is null, want set")
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestReaperRacingReport(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	const n = 40
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 100)

	type started struct {
		id    uuid.UUID
		fence int64
		exec  uuid.UUID
	}
	var jobsList []started
	for i := 0; i < n; i++ {
		id := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
		w := testdb.NewWorker(t, ctx, pool, projectID)
		c := claimOne(t, ctx, pool, queueID, w)
		exec, err := jobs.Start(ctx, pool, id, c.Fence, w)
		testdb.Must(t, err)
		jobsList = append(jobsList, started{id: id, fence: c.Fence, exec: exec.ID})
	}

	testdb.Advance(t, pool, 40*time.Second)

	var mu sync.Mutex
	var fatal []error
	record := func(err error) {
		if err == nil || errors.Is(err, jobs.ErrFenced) {
			return
		}
		mu.Lock()
		fatal = append(fatal, err)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := scheduler.Reap(ctx, pool, 8)
				record(err)
				if err != nil || got == 0 {
					return
				}
			}
		}()
	}
	for _, j := range jobsList {
		wg.Add(1)
		go func(j started) {
			defer wg.Done()
			record(jobs.Complete(ctx, pool, j.id, j.fence, j.exec))
		}(j)
	}
	wg.Wait()

	if len(fatal) != 0 {
		t.Fatalf("errors = %v, want none", fatal)
	}

	var terminal int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from jobs where queue_id = $1 and status in ('completed', 'retry_wait', 'dead_letter')`,
		queueID).Scan(&terminal))
	if terminal != n {
		t.Errorf("resolved jobs = %d, want %d", terminal, n)
	}

	fixed, err := jobs.ReconcileInFlight(ctx, pool)
	testdb.Must(t, err)
	if fixed != 0 {
		t.Errorf("in_flight drift corrected on %d shards, want 0", fixed)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
