package testdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var Epoch = time.Now().UTC().Truncate(time.Second)

func Must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func Base(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	var orgID, projectID, policyID uuid.UUID
	Must(t, pool.QueryRow(ctx,
		`insert into organizations (name) values ($1) returning id`, uuid.NewString()).Scan(&orgID))
	Must(t, pool.QueryRow(ctx,
		`insert into projects (org_id, name) values ($1, $2) returning id`, orgID, uuid.NewString()).Scan(&projectID))
	Must(t, pool.QueryRow(ctx,
		`insert into retry_policies (project_id, name) values ($1, $2) returning id`, projectID, uuid.NewString()).Scan(&policyID))
	return projectID, policyID
}

func NewQueue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, policyID uuid.UUID, maxConcurrency int) uuid.UUID {
	t.Helper()
	var queueID uuid.UUID
	Must(t, pool.QueryRow(ctx, `insert into queues
		(project_id, name, retry_policy_id, max_concurrency, rl_limit_per_sec, rl_burst)
		values ($1, $2, $3, $4, 1000000, 1000000) returning id`,
		projectID, uuid.NewString(), policyID, maxConcurrency).Scan(&queueID))
	return queueID
}

func NewWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID) uuid.UUID {
	t.Helper()
	var workerID uuid.UUID
	Must(t, pool.QueryRow(ctx, `insert into workers
		(project_id, hostname, pid, max_concurrency) values ($1, 'test', 1, 1000) returning id`,
		projectID).Scan(&workerID))
	return workerID
}

func NewJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, queueID, policyID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	Must(t, pool.QueryRow(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, run_at)
		values ($1, $2, 'noop', $3, 'queued', fl.now()) returning id`,
		projectID, queueID, policyID).Scan(&id))
	return id
}

func NewSchedule(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID, cron, tz, overlap, catchup string, nextRunAt time.Time) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	Must(t, pool.QueryRow(ctx, `insert into schedules
		(queue_id, name, cron_expr, timezone, job_type, overlap_policy, catchup_policy, next_run_at)
		values ($1, $2, $3, $4, 'noop', $5, $6, $7) returning id`,
		queueID, uuid.NewString(), cron, tz, overlap, catchup, nextRunAt).Scan(&id))
	return id
}

func NewScheduledJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, queueID, policyID uuid.UUID, delay time.Duration) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	Must(t, pool.QueryRow(ctx, `insert into jobs
		(project_id, queue_id, type, retry_policy_id, status, run_at)
		values ($1, $2, 'noop', $3, 'scheduled', fl.now() + make_interval(secs => $4)) returning id`,
		projectID, queueID, policyID, delay.Seconds()).Scan(&id))
	return id
}

func NewRetryWaitJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, queueID, policyID uuid.UUID, delay time.Duration) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	Must(t, pool.QueryRow(ctx, `insert into jobs
		(project_id, queue_id, type, retry_policy_id, status, run_at)
		values ($1, $2, 'noop', $3, 'retry_wait', fl.now() + make_interval(secs => $4)) returning id`,
		projectID, queueID, policyID, delay.Seconds()).Scan(&id))
	return id
}

func NewExecution(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID, workerID uuid.UUID, outcome string, finishedAt time.Time) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	Must(t, pool.QueryRow(ctx, `insert into job_executions
		(job_id, queue_id, attempt, fence, worker_id, outcome, started_at, finished_at)
		values ($1, $2, 1, 1, $3, $4::exec_outcome, $5, $5) returning id`,
		uuid.New(), queueID, workerID, outcome, finishedAt).Scan(&id))
	return id
}

func NewUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	Must(t, pool.QueryRow(ctx,
		`insert into users (email, password_hash) values ($1, 'x') returning id`,
		uuid.NewString()+"@test").Scan(&id))
	return id
}

func AddMember(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, userID uuid.UUID, role string) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`insert into org_members (org_id, user_id, role) values ($1, $2, $3)`, orgID, userID, role)
	Must(t, err)
}

func NewSession(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID) string {
	t.Helper()
	var id uuid.UUID
	Must(t, pool.QueryRow(ctx,
		`insert into sessions (user_id, expires_at) values ($1, fl.now() + interval '30 days') returning id`,
		userID).Scan(&id))
	return id.String()
}

func Count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	Must(t, pool.QueryRow(ctx, query, args...).Scan(&n))
	return n
}

func JobStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) string {
	t.Helper()
	var s string
	Must(t, pool.QueryRow(ctx, `select status::text from jobs where id = $1`, jobID).Scan(&s))
	return s
}

func SetJitterOff(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	name := pool.Config().ConnConfig.Database
	_, err := pool.Exec(context.Background(), fmt.Sprintf("alter database %s set fl.jitter = 'off'", name))
	Must(t, err)
	pool.Reset()
}

func CheckInvariants(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	var doubleOpen int
	Must(t, pool.QueryRow(ctx, `select count(*) from (
		select job_id from job_executions where finished_at is null
		group by job_id having count(*) > 1) x`).Scan(&doubleOpen))
	if doubleOpen != 0 {
		t.Errorf("jobs with two open executions = %d, want 0", doubleOpen)
	}

	var runningNoLease int
	Must(t, pool.QueryRow(ctx, `select count(*) from jobs j
		where j.status = 'running'
		  and not exists (select 1 from job_leases l where l.job_id = j.id)`).Scan(&runningNoLease))
	if runningNoLease != 0 {
		t.Errorf("running jobs without a lease = %d, want 0", runningNoLease)
	}

	var drift int
	Must(t, pool.QueryRow(ctx, `select count(*) from queue_shards s where s.in_flight <> (
		select count(*) from jobs j where j.queue_id = s.queue_id and j.shard = s.shard
		  and j.status in ('claimed', 'running'))`).Scan(&drift))
	if drift != 0 {
		t.Errorf("shards with in_flight drift = %d, want 0", drift)
	}

	var total, summed int
	Must(t, pool.QueryRow(ctx, `select count(*) from jobs`).Scan(&total))
	Must(t, pool.QueryRow(ctx,
		`select coalesce(sum(c), 0)::int from (select count(*) c from jobs group by status) s`).Scan(&summed))
	if total != summed {
		t.Errorf("status counts sum = %d, want %d", summed, total)
	}
}
