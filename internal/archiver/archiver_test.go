package archiver_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/archiver"
	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func runJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, queueID, policyID uuid.UUID) (uuid.UUID, uuid.UUID) {
	t.Helper()
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
	exec, err := jobs.Start(ctx, pool, jobID, claimed[0].Fence, workerID)
	testdb.Must(t, err)
	return jobID, exec.ID
}

func count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx, query, args...).Scan(&n))
	return n
}

func TestArchive(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	old := testdb.Epoch.AddDate(0, 0, -20).UTC()
	day := old.Format("2006-01-02")
	for _, parent := range []string{"job_logs", "events"} {
		_, err := pool.Exec(ctx, `select fl.ensure_partition($1, $2::date)`, parent, day)
		testdb.Must(t, err)
	}

	testdb.SetNow(t, pool, old)
	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID, execID := runJob(t, ctx, pool, projectID, queueID, policyID)

	_, err := pool.Exec(ctx,
		`insert into job_logs (execution_id, level, message) values ($1, 'info', 'hello')`, execID)
	testdb.Must(t, err)

	var fence int64
	testdb.Must(t, pool.QueryRow(ctx, `select fence from jobs where id = $1`, jobID).Scan(&fence))
	testdb.Must(t, jobs.Complete(ctx, pool, jobID, fence, execID))

	testdb.SetNow(t, pool, testdb.Epoch)
	n, err := archiver.Archive(ctx, pool, 24*time.Hour, 30*24*time.Hour, 100)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("archived = %d, want 1", n)
	}

	if c := count(t, ctx, pool, `select count(*) from jobs where id = $1`, jobID); c != 0 {
		t.Errorf("hot jobs = %d, want 0", c)
	}
	if c := count(t, ctx, pool, `select count(*) from job_executions where id = $1`, execID); c != 0 {
		t.Errorf("hot executions = %d, want 0", c)
	}
	if c := count(t, ctx, pool, `select count(*) from job_logs where execution_id = $1`, execID); c != 0 {
		t.Errorf("hot logs = %d, want 0", c)
	}

	if c := count(t, ctx, pool, `select count(*) from jobs_archive where id = $1`, jobID); c != 1 {
		t.Errorf("archived jobs = %d, want 1", c)
	}
	if c := count(t, ctx, pool, `select count(*) from job_executions_archive where id = $1`, execID); c != 1 {
		t.Errorf("archived executions = %d, want 1", c)
	}
	if c := count(t, ctx, pool, `select count(*) from job_logs_archive where execution_id = $1`, execID); c != 1 {
		t.Errorf("archived logs = %d, want 1", c)
	}

	var status string
	var terminal, finished time.Time
	testdb.Must(t, pool.QueryRow(ctx,
		`select status::text, terminal_at, finished_at from jobs_archive where id = $1`, jobID).
		Scan(&status, &terminal, &finished))
	if status != "completed" {
		t.Errorf("status = %q, want completed", status)
	}
	if !terminal.Equal(finished) {
		t.Errorf("terminal_at = %s, want %s", terminal, finished)
	}

	var outcome string
	var duration int64
	testdb.Must(t, pool.QueryRow(ctx,
		`select outcome::text, duration_ms from job_executions_archive where id = $1`, execID).
		Scan(&outcome, &duration))
	if outcome != "success" {
		t.Errorf("outcome = %q, want success", outcome)
	}

	part := "jobs_archive_" + old.Format("20060102")
	if c := count(t, ctx, pool, `select count(*) from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		join pg_class p on p.oid = i.inhparent
		where p.relname = 'jobs_archive' and c.relname = $1`, part); c != 1 {
		t.Errorf("partition %s present = %d, want 1", part, c)
	}
}

func TestArchiveRecent(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID, execID := runJob(t, ctx, pool, projectID, queueID, policyID)

	var fence int64
	testdb.Must(t, pool.QueryRow(ctx, `select fence from jobs where id = $1`, jobID).Scan(&fence))
	testdb.Must(t, jobs.Complete(ctx, pool, jobID, fence, execID))

	n, err := archiver.Archive(ctx, pool, 24*time.Hour, 30*24*time.Hour, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("archived = %d, want 0", n)
	}
	if c := count(t, ctx, pool, `select count(*) from jobs where id = $1`, jobID); c != 1 {
		t.Errorf("hot jobs = %d, want 1", c)
	}
}

func TestArchiveOpenExecution(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID, _ := runJob(t, ctx, pool, projectID, queueID, policyID)

	_, err := pool.Exec(ctx,
		`update jobs set status = 'completed', finished_at = fl.now(), worker_id = null where id = $1`, jobID)
	testdb.Must(t, err)

	testdb.Advance(t, pool, 48*time.Hour)
	n, err := archiver.Archive(ctx, pool, 24*time.Hour, 30*24*time.Hour, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("archived = %d, want 0", n)
	}
}

func TestArchiveDeadLetter(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	jobID, _ := runJob(t, ctx, pool, projectID, queueID, policyID)

	var fence int64
	testdb.Must(t, pool.QueryRow(ctx, `select fence from jobs where id = $1`, jobID).Scan(&fence))
	testdb.Must(t, jobs.Fail(ctx, pool, jobID, fence, jobs.ErrPermanent))

	testdb.Advance(t, pool, 48*time.Hour)
	n, err := archiver.Archive(ctx, pool, 24*time.Hour, 30*24*time.Hour, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("archived = %d, want 0", n)
	}
	if c := count(t, ctx, pool, `select count(*) from dead_letter_jobs where job_id = $1`, jobID); c != 1 {
		t.Errorf("dead letter rows = %d, want 1", c)
	}

	testdb.Advance(t, pool, 40*24*time.Hour)
	n, err = archiver.Archive(ctx, pool, 24*time.Hour, 30*24*time.Hour, 100)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("archived = %d, want 1", n)
	}
	if c := count(t, ctx, pool, `select count(*) from dead_letter_jobs where job_id = $1`, jobID); c != 0 {
		t.Errorf("hot dead letter rows = %d, want 0", c)
	}
	if c := count(t, ctx, pool, `select count(*) from dead_letter_jobs_archive where job_id = $1`, jobID); c != 1 {
		t.Errorf("archived dead letter rows = %d, want 1", c)
	}
	if c := count(t, ctx, pool, `select count(*) from jobs_archive where id = $1`, jobID); c != 1 {
		t.Errorf("archived jobs = %d, want 1", c)
	}

	var reason string
	testdb.Must(t, pool.QueryRow(ctx,
		`select reason from dead_letter_jobs_archive where job_id = $1`, jobID).Scan(&reason))
	if reason != "permanent_error" {
		t.Errorf("reason = %q, want permanent_error", reason)
	}
}

func TestPrune(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)

	_, err := pool.Exec(ctx, `insert into idempotency_keys (queue_id, key, job_id, fingerprint)
		values ($1, 'stale', $2, 'f'), ($1, 'fresh', $3, 'f')`, queueID, uuid.New(), uuid.New())
	testdb.Must(t, err)
	_, err = pool.Exec(ctx,
		`update idempotency_keys set expires_at = fl.now() - interval '1 hour' where key = 'stale'`)
	testdb.Must(t, err)

	n, err := archiver.Prune(ctx, pool)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("pruned = %d, want 1", n)
	}
	if c := count(t, ctx, pool, `select count(*) from idempotency_keys where queue_id = $1`, queueID); c != 1 {
		t.Errorf("remaining keys = %d, want 1", c)
	}
}
