package api_test

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

func deadLetter(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tn tenant, jobID uuid.UUID) {
	t.Helper()
	worker := testdb.NewWorker(t, ctx, pool, tn.projectID)
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: tn.queueID, WorkerID: worker, FreeSlots: 1, Lease: 30 * time.Second})
	testdb.Must(t, err)
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	_, err = jobs.Start(ctx, pool, jobID, claimed[0].Fence, worker)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Fail(ctx, pool, jobID, claimed[0].Fence, errors.New("boom")))
	if s := testdb.JobStatus(t, ctx, pool, jobID); s != "dead_letter" {
		t.Fatalf("status = %s, want dead_letter", s)
	}
}

func TestDLQReplay(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_attempts = 1 where id = $1`, tn.policyID)
	testdb.Must(t, err)
	memberID, tok := actor(t, ctx, pool, tn.orgID, "member")
	base := server(t, pool)

	jobID := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)
	deadLetter(t, ctx, pool, tn, jobID)

	st, _, raw := do(t, base, tok, "POST", "/dlq/"+jobID.String()+"/replay", nil, nil)
	if st != 200 {
		t.Fatalf("replay = %d, want 200", st)
	}
	m := asMap(t, raw)
	if g := numField(t, m, "replay_generation"); g != 1 {
		t.Fatalf("replay_generation = %d, want 1", g)
	}
	if s := strField(t, m, "status"); s != "queued" {
		t.Fatalf("status = %s, want queued", s)
	}

	if st, _, _ = do(t, base, tok, "POST", "/dlq/"+jobID.String()+"/replay", nil, nil); st != 409 {
		t.Fatalf("second replay = %d, want 409", st)
	}

	worker := testdb.NewWorker(t, ctx, pool, tn.projectID)
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: tn.queueID, WorkerID: worker, FreeSlots: 1, Lease: 30 * time.Second})
	testdb.Must(t, err)
	if len(claimed) != 1 {
		t.Fatalf("claimed after replay = %d, want 1", len(claimed))
	}
	exec, err := jobs.Start(ctx, pool, jobID, claimed[0].Fence, worker)
	if err != nil {
		t.Fatalf("start after replay: %v", err)
	}
	if exec.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", exec.Attempt)
	}
	testdb.Must(t, jobs.Complete(ctx, pool, jobID, claimed[0].Fence, exec.ID))

	var execs int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from job_executions where job_id = $1`, jobID).Scan(&execs))
	if execs != 2 {
		t.Fatalf("executions = %d, want 2", execs)
	}
	var gens int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(distinct replay_generation) from job_executions where job_id = $1`, jobID).Scan(&gens))
	if gens != 2 {
		t.Fatalf("distinct replay generations = %d, want 2", gens)
	}
	if s := testdb.JobStatus(t, ctx, pool, jobID); s != "completed" {
		t.Fatalf("final status = %s, want completed", s)
	}

	var replayed int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from dead_letter_jobs where job_id = $1 and replayed_by = $2`,
		jobID, memberID).Scan(&replayed))
	if replayed != 1 {
		t.Fatalf("dlq rows replayed by member = %d, want 1", replayed)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
