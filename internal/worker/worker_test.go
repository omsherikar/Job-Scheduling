package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

		CompleteBatch: 25,
		CompleteWait:  20 * time.Millisecond,
		Heartbeat:     time.Second,
	}
}

func TestWorkerLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error { return nil },
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return testdb.JobStatus(t, ctx, pool, jobID) == "completed" }) {
		t.Fatalf("status = %q, want completed", testdb.JobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var executions, attempt int
	var outcome string
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from job_executions where job_id = $1`, jobID).Scan(&executions))
	testdb.Must(t, pool.QueryRow(ctx,
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

	testdb.CheckInvariants(t, ctx, pool)
}

func TestHandlerErrorRetries(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetJitterOff(t, pool)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error { return errors.New("boom") },
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return testdb.JobStatus(t, ctx, pool, jobID) == "retry_wait" }) {
		t.Fatalf("status = %q, want retry_wait", testdb.JobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var attemptCount int
	var outcome string
	var runAt, finishedAt time.Time
	testdb.Must(t, pool.QueryRow(ctx, `select attempt_count, run_at from jobs where id = $1`, jobID).Scan(&attemptCount, &runAt))
	testdb.Must(t, pool.QueryRow(ctx,
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

	testdb.CheckInvariants(t, ctx, pool)
}

func TestHandlerErrorDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetJitterOff(t, pool)

	projectID, policyID := testdb.Base(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_attempts = 1 where id = $1`, policyID)
	testdb.Must(t, err)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error { return errors.New("boom") },
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return testdb.JobStatus(t, ctx, pool, jobID) == "dead_letter" }) {
		t.Fatalf("status = %q, want dead_letter", testdb.JobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var reason string
	var historyLen int
	testdb.Must(t, pool.QueryRow(ctx,
		`select reason, jsonb_array_length(execution_history) from dead_letter_jobs where job_id = $1`,
		jobID).Scan(&reason, &historyLen))
	if reason != "attempts_exhausted" {
		t.Errorf("reason = %q, want attempts_exhausted", reason)
	}
	if historyLen != 1 {
		t.Errorf("execution_history length = %d, want 1", historyLen)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestPermanentErrorDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetJitterOff(t, pool)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			return fmt.Errorf("bad input: %w", jobs.ErrPermanent)
		},
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool { return testdb.JobStatus(t, ctx, pool, jobID) == "dead_letter" }) {
		t.Fatalf("status = %q, want dead_letter", testdb.JobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var reason string
	var attemptCount int
	var outcome string
	testdb.Must(t, pool.QueryRow(ctx, `select reason from dead_letter_jobs where job_id = $1`, jobID).Scan(&reason))
	testdb.Must(t, pool.QueryRow(ctx, `select attempt_count from jobs where id = $1`, jobID).Scan(&attemptCount))
	testdb.Must(t, pool.QueryRow(ctx,
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

	testdb.CheckInvariants(t, ctx, pool)
}

func TestGracefulShutdown(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	workerID := testdb.NewWorker(t, ctx, pool, projectID)
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

	testdb.Advance(t, pool, 40*time.Second)
	reaped, err := scheduler.Reap(ctx, pool, 10)
	testdb.Must(t, err)
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}

	var status string
	var attemptCount int
	testdb.Must(t, pool.QueryRow(ctx,
		`select status::text, attempt_count from jobs where id = $1`, jobID).Scan(&status, &attemptCount))
	if status != "retry_wait" {
		t.Errorf("status = %q, want retry_wait", status)
	}
	if attemptCount != 0 {
		t.Errorf("attempt_count = %d, want 0", attemptCount)
	}

	var executions int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from job_executions where job_id = $1`, jobID).Scan(&executions))
	if executions != 0 {
		t.Errorf("executions = %d, want 0", executions)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestCancelClosesExecution(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

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
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `delete from job_leases where job_id = $1`, jobID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `select fl.queue_release($1, 1)`, queueID)
	testdb.Must(t, err)

	closed := waitFor(3*time.Second, func() bool {
		var open int
		testdb.Must(t, pool.QueryRow(ctx,
			`select count(*) from job_executions where job_id = $1 and finished_at is null`, jobID).Scan(&open))
		return open == 0
	})
	cancel()
	<-done

	if !closed {
		t.Error("execution finished_at = nil, want set")
	}

	var outcome *string
	testdb.Must(t, pool.QueryRow(ctx,
		`select outcome::text from job_executions where job_id = $1`, jobID).Scan(&outcome))
	if outcome == nil || *outcome != "cancelled" {
		got := "<nil>"
		if outcome != nil {
			got = *outcome
		}
		t.Errorf("outcome = %q, want cancelled", got)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestProgressOnHeartbeat(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	reported := make(chan struct{})
	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			j.Report(map[string]any{"done": 7, "total": 10})
			close(reported)
			<-ctx.Done()
			return nil
		},
	}

	cfg := worker.Config{
		QueueID:     queueID,
		WorkerID:    workerID,
		Concurrency: 1,
		Lease:       300 * time.Millisecond,
		Drain:       200 * time.Millisecond,
		Poll:        20 * time.Millisecond,

		CompleteBatch: 25,
		CompleteWait:  20 * time.Millisecond,
		Heartbeat:     time.Second,
	}

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		worker.Run(runCtx, pool, cfg, handlers)
		close(done)
	}()

	<-reported

	var progress []byte
	waitFor(5*time.Second, func() bool {
		err := pool.QueryRow(ctx, `select progress from job_leases where job_id = $1`, jobID).Scan(&progress)
		return err == nil && progress != nil
	})
	stop()
	<-done

	if progress == nil {
		t.Fatal("lease progress is null, want the reported value")
	}
	var got map[string]any
	testdb.Must(t, json.Unmarshal(progress, &got))
	if got["done"] != float64(7) {
		t.Errorf("done = %v, want 7", got["done"])
	}
}

func TestHandlerPanic(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	boom := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
	fine := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	var done sync.WaitGroup
	done.Add(2)
	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			defer done.Done()
			if j.ID == boom {
				panic("nil map write")
			}
			return nil
		},
	}

	cfg := worker.Config{
		QueueID:     queueID,
		WorkerID:    workerID,
		Concurrency: 2,
		Lease:       30 * time.Second,
		Drain:       200 * time.Millisecond,
		Poll:        20 * time.Millisecond,

		CompleteBatch: 25,
		CompleteWait:  20 * time.Millisecond,
		Heartbeat:     time.Second,
	}
	stop, errs := run(t, pool, cfg, handlers)
	done.Wait()

	if !waitFor(5*time.Second, func() bool {
		return testdb.JobStatus(t, ctx, pool, boom) == "retry_wait" &&
			testdb.JobStatus(t, ctx, pool, fine) == "completed"
	}) {
		t.Fatalf("statuses = %s, %s, want retry_wait, completed",
			testdb.JobStatus(t, ctx, pool, boom), testdb.JobStatus(t, ctx, pool, fine))
	}

	var msg string
	testdb.Must(t, pool.QueryRow(ctx,
		`select error_message from job_executions where job_id = $1`, boom).Scan(&msg))
	if !strings.Contains(msg, "handler panic") {
		t.Errorf("error message = %q, want contains %q", msg, "handler panic")
	}

	stop()
	<-errs
	testdb.CheckInvariants(t, ctx, pool)
}

func TestHandlerLogs(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			j.Log("info", "starting")
			j.Log("warn", "halfway")
			return nil
		},
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool {
		return testdb.JobStatus(t, ctx, pool, jobID) == "completed"
	}) {
		t.Fatalf("status = %q, want completed", testdb.JobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	rows, err := pool.Query(ctx, `select l.level, l.message from job_logs l
		join job_executions x on x.id = l.execution_id
		where x.job_id = $1 order by l.ts, l.id`, jobID)
	testdb.Must(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var level, message string
		testdb.Must(t, rows.Scan(&level, &message))
		got = append(got, level+" "+message)
	}
	testdb.Must(t, rows.Err())

	want := []string{"info starting", "warn halfway"}
	if len(got) != len(want) {
		t.Fatalf("log lines = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSnoozeKeepsAttempts(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			return worker.Snooze{After: time.Minute}
		},
	}
	cancel, done := run(t, pool, config(queueID, workerID, 30*time.Second), handlers)

	if !waitFor(5*time.Second, func() bool {
		return testdb.JobStatus(t, ctx, pool, jobID) == "scheduled"
	}) {
		t.Fatalf("status = %q, want scheduled", testdb.JobStatus(t, ctx, pool, jobID))
	}
	cancel()
	<-done

	var attempts, snoozes int
	var runAt, now time.Time
	testdb.Must(t, pool.QueryRow(ctx,
		`select attempt_count, snooze_count, run_at, fl.now() from jobs where id = $1`, jobID).
		Scan(&attempts, &snoozes, &runAt, &now))
	if attempts != 1 {
		t.Errorf("attempt_count = %d, want 1", attempts)
	}
	if snoozes != 1 {
		t.Errorf("snooze_count = %d, want 1", snoozes)
	}
	if !runAt.After(now) {
		t.Errorf("run_at = %s, want after %s", runAt, now)
	}

	var outcome string
	testdb.Must(t, pool.QueryRow(ctx,
		`select outcome::text from job_executions where job_id = $1`, jobID).Scan(&outcome))
	if outcome != "snoozed" {
		t.Errorf("outcome = %q, want snoozed", outcome)
	}

	var inFlight int
	testdb.Must(t, pool.QueryRow(ctx, `select in_flight from queues where id = $1`, queueID).Scan(&inFlight))
	if inFlight != 0 {
		t.Errorf("in_flight = %d, want 0", inFlight)
	}
	testdb.CheckInvariants(t, ctx, pool)
}

func TestSnoozeClampedToPolicy(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_delay_ms = 5000 where id = $1`, policyID)
	testdb.Must(t, err)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: queueID, WorkerID: workerID, FreeSlots: 1, Lease: time.Hour})
	testdb.Must(t, err)
	exec, err := jobs.Start(ctx, pool, jobID, claimed[0].Fence, workerID)
	testdb.Must(t, err)

	testdb.Must(t, jobs.Snooze(ctx, pool, jobID, claimed[0].Fence, exec.ID, 365*24*time.Hour))

	var seconds float64
	testdb.Must(t, pool.QueryRow(ctx,
		`select extract(epoch from (run_at - fl.now())) from jobs where id = $1`, jobID).Scan(&seconds))
	if seconds > 6 {
		t.Errorf("snooze delay = %.0fs, want <= 5s", seconds)
	}
}

func TestSnoozeExhausted(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_attempts = 2 where id = $1`, policyID)
	testdb.Must(t, err)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)
	jobID := testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)

	wake := func() jobs.Claimed {
		t.Helper()
		_, err := pool.Exec(ctx,
			`update jobs set status = 'queued', run_at = fl.now() where id = $1 and status = 'scheduled'`, jobID)
		testdb.Must(t, err)
		claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
			QueueID: queueID, WorkerID: workerID, FreeSlots: 1, Lease: time.Hour})
		testdb.Must(t, err)
		if len(claimed) != 1 {
			t.Fatalf("claimed = %d, want 1", len(claimed))
		}
		return claimed[0]
	}

	for i := 0; i < 2; i++ {
		c := wake()
		exec, err := jobs.Start(ctx, pool, jobID, c.Fence, workerID)
		testdb.Must(t, err)
		testdb.Must(t, jobs.Snooze(ctx, pool, jobID, c.Fence, exec.ID, time.Millisecond))
	}

	c := wake()
	exec, err := jobs.Start(ctx, pool, jobID, c.Fence, workerID)
	testdb.Must(t, err)

	err = jobs.Snooze(ctx, pool, jobID, c.Fence, exec.ID, time.Millisecond)
	if !errors.Is(err, jobs.ErrFenced) {
		t.Fatalf("third snooze err = %v, want %v", err, jobs.ErrFenced)
	}

	var snoozes int
	testdb.Must(t, pool.QueryRow(ctx, `select snooze_count from jobs where id = $1`, jobID).Scan(&snoozes))
	if snoozes != 2 {
		t.Errorf("snooze_count = %d, want 2", snoozes)
	}
	if s := testdb.JobStatus(t, ctx, pool, jobID); s != "running" {
		t.Errorf("status = %q, want running", s)
	}
}

func TestRegister(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)

	workerID, err := worker.Register(ctx, pool, queueID, 4)
	testdb.Must(t, err)

	var project uuid.UUID
	var state string
	var concurrency int
	testdb.Must(t, pool.QueryRow(ctx,
		`select project_id, state, max_concurrency from workers where id = $1`, workerID).
		Scan(&project, &state, &concurrency))
	if project != projectID {
		t.Errorf("project = %v, want %v", project, projectID)
	}
	if state != "active" {
		t.Errorf("state = %q, want active", state)
	}
	if concurrency != 4 {
		t.Errorf("max_concurrency = %d, want 4", concurrency)
	}

	cfg := config(queueID, workerID, 30*time.Second)
	cfg.Heartbeat = 20 * time.Millisecond
	cancel, done := run(t, pool, cfg, map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error { return nil },
	})

	live := func() int {
		var n int
		testdb.Must(t, pool.QueryRow(ctx, `select count(*) from workers w
			join worker_queues wq on wq.worker_id = w.id
			where wq.queue_id = $1 and w.state = 'active'`, queueID).Scan(&n))
		return n
	}
	if !waitFor(5*time.Second, func() bool { return live() == 1 }) {
		t.Fatalf("live workers = %d, want 1", live())
	}

	var handlers []string
	testdb.Must(t, pool.QueryRow(ctx, `select handlers from workers where id = $1`, workerID).Scan(&handlers))
	if len(handlers) != 1 || handlers[0] != "noop" {
		t.Errorf("handlers = %v, want [noop]", handlers)
	}

	seen := func() time.Time {
		var at time.Time
		testdb.Must(t, pool.QueryRow(ctx,
			`select last_seen_at from workers where id = $1`, workerID).Scan(&at))
		return at
	}
	first := seen()
	testdb.Advance(t, pool, time.Minute)
	if !waitFor(5*time.Second, func() bool { return seen().After(first) }) {
		t.Errorf("last_seen_at = %v, want after %v", seen(), first)
	}

	cancel()
	<-done

	testdb.Must(t, pool.QueryRow(ctx, `select state from workers where id = $1`, workerID).Scan(&state))
	if state != "dead" {
		t.Errorf("state after shutdown = %q, want dead", state)
	}
}

func TestRegisterUnknownQueue(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	_, err := worker.Register(ctx, pool, uuid.New(), 4)
	if !errors.Is(err, worker.ErrNoQueue) {
		t.Fatalf("err = %v, want %v", err, worker.ErrNoQueue)
	}
}

func TestListenStartsSooner(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 4)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	started := make(chan struct{}, 1)
	handlers := map[string]worker.Handler{
		"noop": func(ctx context.Context, j worker.Job) error {
			select {
			case started <- struct{}{}:
			default:
			}
			return nil
		},
	}

	cfg := config(queueID, workerID, 30*time.Second)
	cfg.Poll = 30 * time.Second
	cfg.Listen = true
	cancel, done := run(t, pool, cfg, handlers)
	defer func() { cancel(); <-done }()

	time.Sleep(200 * time.Millisecond)
	testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
	_, err := pool.Exec(ctx, `select pg_notify('fl_events', '')`)
	testdb.Must(t, err)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no start within 5s, want one")
	}
}
