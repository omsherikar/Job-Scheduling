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

var epoch = time.Now().UTC().Truncate(time.Second)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func base(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	var orgID, projectID, policyID uuid.UUID
	must(t, pool.QueryRow(ctx,
		`insert into organizations (name) values ($1) returning id`, uuid.NewString()).Scan(&orgID))
	must(t, pool.QueryRow(ctx,
		`insert into projects (org_id, name) values ($1, $2) returning id`, orgID, uuid.NewString()).Scan(&projectID))
	must(t, pool.QueryRow(ctx,
		`insert into retry_policies (project_id, name) values ($1, $2) returning id`, projectID, uuid.NewString()).Scan(&policyID))
	return projectID, policyID
}

func newQueue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, policyID uuid.UUID, maxConcurrency int) uuid.UUID {
	t.Helper()
	var queueID uuid.UUID
	must(t, pool.QueryRow(ctx, `insert into queues
		(project_id, name, retry_policy_id, max_concurrency, rl_limit_per_sec, rl_burst, rl_tokens)
		values ($1, $2, $3, $4, 1000000, 1000000, 1000000) returning id`,
		projectID, uuid.NewString(), policyID, maxConcurrency).Scan(&queueID))
	return queueID
}

func newWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID) uuid.UUID {
	t.Helper()
	var workerID uuid.UUID
	must(t, pool.QueryRow(ctx, `insert into workers
		(project_id, hostname, pid, max_concurrency) values ($1, 'test', 1, 1000) returning id`,
		projectID).Scan(&workerID))
	return workerID
}

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

func checkInvariants(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	var doubleOpen int
	must(t, pool.QueryRow(ctx, `select count(*) from (
		select job_id from job_executions where finished_at is null
		group by job_id having count(*) > 1) x`).Scan(&doubleOpen))
	if doubleOpen != 0 {
		t.Errorf("jobs with two open executions = %d, want 0", doubleOpen)
	}

	var runningNoLease int
	must(t, pool.QueryRow(ctx, `select count(*) from jobs j
		where j.status = 'running'
		  and not exists (select 1 from job_leases l where l.job_id = j.id)`).Scan(&runningNoLease))
	if runningNoLease != 0 {
		t.Errorf("running jobs without a lease = %d, want 0", runningNoLease)
	}

	var drift int
	must(t, pool.QueryRow(ctx, `select count(*) from queues q where q.in_flight <> (
		select count(*) from jobs j where j.queue_id = q.id and j.status in ('claimed', 'running'))`).Scan(&drift))
	if drift != 0 {
		t.Errorf("queues with in_flight drift = %d, want 0", drift)
	}

	var total, summed int
	must(t, pool.QueryRow(ctx, `select count(*) from jobs`).Scan(&total))
	must(t, pool.QueryRow(ctx,
		`select coalesce(sum(c), 0)::int from (select count(*) c from jobs group by status) s`).Scan(&summed))
	if total != summed {
		t.Errorf("status counts sum = %d, want %d", summed, total)
	}
}

func TestReaperRecoversCrash(t *testing.T) {
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

	var status string
	var attemptCount int
	var inFlight int
	var leases int
	must(t, pool.QueryRow(ctx,
		`select status::text, attempt_count from jobs where id = $1`, jobID).Scan(&status, &attemptCount))
	must(t, pool.QueryRow(ctx, `select in_flight from queues where id = $1`, queueID).Scan(&inFlight))
	must(t, pool.QueryRow(ctx, `select count(*) from job_leases where job_id = $1`, jobID).Scan(&leases))

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
	must(t, pool.QueryRow(ctx,
		`select outcome::text, finished_at is not null from job_executions where id = $1`,
		execA.ID).Scan(&outcome, &finished))
	if outcome != "lost" {
		t.Errorf("execution outcome = %q, want lost", outcome)
	}
	if !finished {
		t.Error("execution finished_at is null, want set")
	}

	promote(t, ctx, pool, jobID)
	workerB := newWorker(t, ctx, pool, projectID)
	b := claimOne(t, ctx, pool, queueID, workerB)
	if b.Fence <= a.Fence {
		t.Fatalf("reclaim fence = %d, want > %d", b.Fence, a.Fence)
	}
	execB, err := jobs.Start(ctx, pool, jobID, b.Fence, workerB)
	must(t, err)
	must(t, jobs.Complete(ctx, pool, jobID, b.Fence, execB.ID))

	if got := jobStatus(t, ctx, pool, jobID); got != "completed" {
		t.Errorf("final status = %q, want completed", got)
	}

	checkInvariants(t, ctx, pool)
}

func TestReaperDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, epoch)

	projectID, policyID := base(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_attempts = 1 where id = $1`, policyID)
	must(t, err)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	w := newWorker(t, ctx, pool, projectID)
	c := claimOne(t, ctx, pool, queueID, w)
	execID, err := jobs.Start(ctx, pool, jobID, c.Fence, w)
	must(t, err)

	testdb.Advance(t, pool, 40*time.Second)
	reaped, err := scheduler.Reap(ctx, pool, 10)
	must(t, err)
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}

	if got := jobStatus(t, ctx, pool, jobID); got != "dead_letter" {
		t.Errorf("status = %q, want dead_letter", got)
	}

	var reason string
	var historyLen int
	must(t, pool.QueryRow(ctx,
		`select reason, jsonb_array_length(execution_history) from dead_letter_jobs where job_id = $1`,
		jobID).Scan(&reason, &historyLen))
	if reason != "lost_exhausted" {
		t.Errorf("reason = %q, want lost_exhausted", reason)
	}
	if historyLen != 1 {
		t.Errorf("execution_history length = %d, want 1", historyLen)
	}

	var outcome string
	must(t, pool.QueryRow(ctx,
		`select outcome::text from job_executions where id = $1`, execID.ID).Scan(&outcome))
	if outcome != "lost" {
		t.Errorf("execution outcome = %q, want lost", outcome)
	}

	var inFlight int
	must(t, pool.QueryRow(ctx, `select in_flight from queues where id = $1`, queueID).Scan(&inFlight))
	if inFlight != 0 {
		t.Errorf("in_flight = %d, want 0", inFlight)
	}

	checkInvariants(t, ctx, pool)
}

func TestSweepGrace(t *testing.T) {
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
		`update jobs set status = 'completed', finished_at = fl.now(), worker_id = null where id = $1`, jobID)
	must(t, err)
	_, err = pool.Exec(ctx, `delete from job_leases where job_id = $1`, jobID)
	must(t, err)
	_, err = pool.Exec(ctx, `select fl.queue_release($1, 1)`, queueID)
	must(t, err)

	swept, err := scheduler.SweepOrphanExecutions(ctx, pool)
	must(t, err)
	if swept != 0 {
		t.Fatalf("swept within grace = %d, want 0", swept)
	}
	var open int
	must(t, pool.QueryRow(ctx,
		`select count(*) from job_executions where job_id = $1 and finished_at is null`, jobID).Scan(&open))
	if open != 1 {
		t.Fatalf("open executions within grace = %d, want 1", open)
	}

	testdb.Advance(t, pool, 11*time.Minute)
	swept, err = scheduler.SweepOrphanExecutions(ctx, pool)
	must(t, err)
	if swept != 1 {
		t.Fatalf("swept past grace = %d, want 1", swept)
	}

	var outcome string
	var finished bool
	must(t, pool.QueryRow(ctx,
		`select outcome::text, finished_at is not null from job_executions where job_id = $1`,
		jobID).Scan(&outcome, &finished))
	if outcome != "lost" {
		t.Errorf("execution outcome = %q, want lost", outcome)
	}
	if !finished {
		t.Error("execution finished_at is null, want set")
	}

	checkInvariants(t, ctx, pool)
}

func TestReaperRacingReport(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, epoch)

	projectID, policyID := base(t, ctx, pool)
	const n = 40
	queueID := newQueue(t, ctx, pool, projectID, policyID, 100)

	type started struct {
		id    uuid.UUID
		fence int64
		exec  uuid.UUID
	}
	var jobsList []started
	for i := 0; i < n; i++ {
		id := newJob(t, ctx, pool, projectID, queueID, policyID)
		w := newWorker(t, ctx, pool, projectID)
		c := claimOne(t, ctx, pool, queueID, w)
		exec, err := jobs.Start(ctx, pool, id, c.Fence, w)
		must(t, err)
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
	must(t, pool.QueryRow(ctx,
		`select count(*) from jobs where queue_id = $1 and status in ('completed', 'retry_wait', 'dead_letter')`,
		queueID).Scan(&terminal))
	if terminal != n {
		t.Errorf("resolved jobs = %d, want %d", terminal, n)
	}

	fixed, err := jobs.ReconcileInFlight(ctx, pool)
	must(t, err)
	if fixed != 0 {
		t.Errorf("in_flight drift corrected on %d queues, want 0", fixed)
	}

	checkInvariants(t, ctx, pool)
}

func jobStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) string {
	t.Helper()
	var s string
	must(t, pool.QueryRow(ctx, `select status::text from jobs where id = $1`, jobID).Scan(&s))
	return s
}
