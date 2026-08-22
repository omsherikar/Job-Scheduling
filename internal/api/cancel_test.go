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

func TestCancel(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, tok := actor(t, ctx, pool, tn.orgID, "member")
	base := server(t, pool)

	retryWait := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)
	worker := testdb.NewWorker(t, ctx, pool, tn.projectID)
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: tn.queueID, WorkerID: worker, FreeSlots: 1, Lease: 30 * time.Second})
	testdb.Must(t, err)
	_, err = jobs.Start(ctx, pool, retryWait, claimed[0].Fence, worker)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Fail(ctx, pool, retryWait, claimed[0].Fence, errors.New("boom")))
	if s := testdb.JobStatus(t, ctx, pool, retryWait); s != "retry_wait" {
		t.Fatalf("status = %s, want retry_wait", s)
	}

	queued := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)

	var scheduled uuid.UUID
	testdb.Must(t, pool.QueryRow(ctx,
		`insert into jobs (project_id, queue_id, type, retry_policy_id, status, run_at)
		 values ($1, $2, 'noop', $3, 'scheduled', fl.now() + interval '1 hour') returning id`,
		tn.projectID, tn.queueID, tn.policyID).Scan(&scheduled))

	for _, c := range []struct {
		name string
		id   uuid.UUID
	}{
		{"queued", queued},
		{"scheduled", scheduled},
		{"retry_wait", retryWait},
	} {
		t.Run(c.name, func(t *testing.T) {
			st, _, raw := do(t, base, tok, "POST", "/jobs/"+c.id.String()+"/cancel", nil, nil)
			if st != 200 {
				t.Fatalf("cancel = %d, want 200", st)
			}
			if s := strField(t, asMap(t, raw), "status"); s != "cancelled" {
				t.Fatalf("status = %s, want cancelled", s)
			}
			if st, _, _ := do(t, base, tok, "POST", "/jobs/"+c.id.String()+"/cancel", nil, nil); st != 409 {
				t.Fatalf("second cancel = %d, want 409", st)
			}
		})
	}

	t.Run("running", func(t *testing.T) {
		running := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)
		w := testdb.NewWorker(t, ctx, pool, tn.projectID)
		cl, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
			QueueID: tn.queueID, WorkerID: w, FreeSlots: 1, Lease: 30 * time.Second})
		testdb.Must(t, err)
		_, err = jobs.Start(ctx, pool, running, cl[0].Fence, w)
		testdb.Must(t, err)
		if f := inFlight(t, ctx, pool, tn.queueID); f != 1 {
			t.Fatalf("in_flight before cancel = %d, want 1", f)
		}
		if st, _, _ := do(t, base, tok, "POST", "/jobs/"+running.String()+"/cancel", nil, nil); st != 200 {
			t.Fatalf("cancel = %d, want 200", st)
		}
		if s := testdb.JobStatus(t, ctx, pool, running); s != "cancelled" {
			t.Fatalf("status = %s, want cancelled", s)
		}
		if f := inFlight(t, ctx, pool, tn.queueID); f != 0 {
			t.Fatalf("in_flight after cancel = %d, want 0", f)
		}
	})

	testdb.CheckInvariants(t, ctx, pool)
}

func inFlight(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID) int {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx, `select in_flight from queues where id = $1`, queueID).Scan(&n))
	return n
}
