package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
	"github.com/shrutu0929/fenceline/internal/worker"
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

func setJitterOff(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	name := pool.Config().ConnConfig.Database
	_, err := pool.Exec(context.Background(), fmt.Sprintf("alter database %s set fl.jitter = 'off'", name))
	must(t, err)
	pool.Reset()
}

func jobStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) string {
	t.Helper()
	var s string
	must(t, pool.QueryRow(ctx, `select status::text from jobs where id = $1`, jobID).Scan(&s))
	return s
}

func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
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

func run(t *testing.T, pool *pgxpool.Pool, cfg worker.Config, handlers map[string]worker.Handler) (context.CancelFunc, chan error) {
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(runCtx, pool, cfg, handlers)
	}()
	return cancel, done
}

func config(queueID, workerID uuid.UUID, lease time.Duration) worker.Config {
	return worker.Config{
		QueueID:     queueID,
		WorkerID:    workerID,
		Concurrency: 1,
		Lease:       lease,
		Drain:       2 * time.Second,
		Poll:        30 * time.Millisecond,
	}
}

func TestWorkerLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := newWorker(t, ctx, pool, projectID)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error { return nil },
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return jobStatus(t, ctx, pool, jobID) == "completed" }) {
		t.Fatalf("status = %q, want completed", jobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var executions, attempt int
	var outcome string
	must(t, pool.QueryRow(ctx, `select count(*) from job_executions where job_id = $1`, jobID).Scan(&executions))
	must(t, pool.QueryRow(ctx,
		`select attempt, outcome::text from job_executions where job_id = $1`, jobID).Scan(&attempt, &outcome))
	if executions != 1 {
		t.Errorf("executions = %d, want 1", executions)
	}
	if outcome != "success" {
		t.Errorf("outcome = %q, want success", outcome)
	}
	if attempt != 1 {
		t.Errorf("attempt = %d, want 1", attempt)
	}

	checkInvariants(t, ctx, pool)
}

func TestHandlerErrorRetries(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	setJitterOff(t, pool)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := newWorker(t, ctx, pool, projectID)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error { return errors.New("boom") },
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return jobStatus(t, ctx, pool, jobID) == "retry_wait" }) {
		t.Fatalf("status = %q, want retry_wait", jobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var attemptCount int
	var outcome string
	var runAt, finishedAt time.Time
	must(t, pool.QueryRow(ctx, `select attempt_count, run_at from jobs where id = $1`, jobID).Scan(&attemptCount, &runAt))
	must(t, pool.QueryRow(ctx,
		`select outcome::text, finished_at from job_executions where job_id = $1`, jobID).Scan(&outcome, &finishedAt))

	if attemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1", attemptCount)
	}
	if outcome != "retryable_error" {
		t.Errorf("outcome = %q, want retryable_error", outcome)
	}
	if !runAt.After(finishedAt) {
		t.Errorf("run_at = %v, want after finished_at %v", runAt, finishedAt)
	}

	checkInvariants(t, ctx, pool)
}

func TestHandlerErrorDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	setJitterOff(t, pool)

	projectID, policyID := base(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_attempts = 1 where id = $1`, policyID)
	must(t, err)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := newWorker(t, ctx, pool, projectID)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error { return errors.New("boom") },
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return jobStatus(t, ctx, pool, jobID) == "dead_letter" }) {
		t.Fatalf("status = %q, want dead_letter", jobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var reason string
	var historyLen int
	must(t, pool.QueryRow(ctx,
		`select reason, jsonb_array_length(execution_history) from dead_letter_jobs where job_id = $1`,
		jobID).Scan(&reason, &historyLen))
	if reason != "attempts_exhausted" {
		t.Errorf("reason = %q, want attempts_exhausted", reason)
	}
	if historyLen != 1 {
		t.Errorf("execution_history length = %d, want 1", historyLen)
	}

	checkInvariants(t, ctx, pool)
}

func TestPermanentErrorDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	setJitterOff(t, pool)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := newWorker(t, ctx, pool, projectID)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			return fmt.Errorf("bad input: %w", jobs.ErrPermanent)
		},
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return jobStatus(t, ctx, pool, jobID) == "dead_letter" }) {
		t.Fatalf("status = %q, want dead_letter", jobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var reason string
	var attemptCount int
	var outcome string
	must(t, pool.QueryRow(ctx, `select reason from dead_letter_jobs where job_id = $1`, jobID).Scan(&reason))
	must(t, pool.QueryRow(ctx, `select attempt_count from jobs where id = $1`, jobID).Scan(&attemptCount))
	must(t, pool.QueryRow(ctx,
		`select outcome::text from job_executions where job_id = $1`, jobID).Scan(&outcome))

	if reason != "permanent_error" {
		t.Errorf("reason = %q, want permanent_error", reason)
	}
	if attemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1", attemptCount)
	}
	if outcome != "permanent_error" {
		t.Errorf("outcome = %q, want permanent_error", outcome)
	}

	checkInvariants(t, ctx, pool)
}

func TestGracefulShutdown(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, epoch)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	workerID := newWorker(t, ctx, pool, projectID)
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

	testdb.Advance(t, pool, 40*time.Second)
	reaped, err := scheduler.Reap(ctx, pool, 10)
	must(t, err)
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}

	var status string
	var attemptCount int
	must(t, pool.QueryRow(ctx,
		`select status::text, attempt_count from jobs where id = $1`, jobID).Scan(&status, &attemptCount))
	if status != "retry_wait" {
		t.Errorf("status = %q, want retry_wait", status)
	}
	if attemptCount != 0 {
		t.Errorf("attempt_count = %d, want 0", attemptCount)
	}

	var executions int
	must(t, pool.QueryRow(ctx, `select count(*) from job_executions where job_id = $1`, jobID).Scan(&executions))
	if executions != 0 {
		t.Errorf("executions = %d, want 0", executions)
	}

	checkInvariants(t, ctx, pool)
}

func TestCancelClosesExecution(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := base(t, ctx, pool)
	queueID := newQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := newWorker(t, ctx, pool, projectID)
	jobID := newJob(t, ctx, pool, projectID, queueID, policyID)

	var once sync.Once
	running := make(chan struct{})
	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			once.Do(func() { close(running) })
			<-ctx.Done()
			return ctx.Err()
		},
	}
	cancel, done := run(t, pool, config(queueID, workerID, 300*time.Millisecond), handlers)

	<-running
	_, err := pool.Exec(ctx,
		`update jobs set status = 'cancelled', finished_at = fl.now(), worker_id = null where id = $1`, jobID)
	must(t, err)
	_, err = pool.Exec(ctx, `delete from job_leases where job_id = $1`, jobID)
	must(t, err)
	_, err = pool.Exec(ctx, `select fl.queue_release($1, 1)`, queueID)
	must(t, err)

	closed := waitFor(3*time.Second, func() bool {
		var open int
		must(t, pool.QueryRow(ctx,
			`select count(*) from job_executions where job_id = $1 and finished_at is null`, jobID).Scan(&open))
		return open == 0
	})
	cancel()
	<-done

	if !closed {
		t.Error("execution finished_at = nil, want set")
	}

	var outcome *string
	must(t, pool.QueryRow(ctx,
		`select outcome::text from job_executions where job_id = $1`, jobID).Scan(&outcome))
	if outcome == nil || *outcome != "cancelled" {
		got := "<nil>"
		if outcome != nil {
			got = *outcome
		}
		t.Errorf("outcome = %q, want cancelled", got)
	}

	checkInvariants(t, ctx, pool)
}
