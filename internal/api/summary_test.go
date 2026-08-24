package api_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func failJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tn tenant, msg string) {
	t.Helper()
	id := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)
	worker := testdb.NewWorker(t, ctx, pool, tn.projectID)
	cl, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: tn.queueID, WorkerID: worker, FreeSlots: 1, Lease: 30 * time.Second})
	testdb.Must(t, err)
	_, err = jobs.Start(ctx, pool, id, cl[0].Fence, worker)
	testdb.Must(t, err)
	testdb.Must(t, jobs.Fail(ctx, pool, id, cl[0].Fence, errors.New(msg)))
}

func TestFailureSummary(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, tok := actor(t, ctx, pool, tn.orgID, "viewer")
	base := server(t, pool)

	for i := 0; i < 3; i++ {
		failJob(t, ctx, pool, tn, "dial tcp 10.0.0.4:5432: connection refused")
	}
	failJob(t, ctx, pool, tn, "payload missing field customer_id")

	st, _, raw := do(t, base, tok, "GET", "/queues/"+tn.queueID.String()+"/failure-summary", nil, nil)
	if st != 200 {
		t.Fatalf("status = %d, want 200: %s", st, raw)
	}
	body := asMap(t, raw)
	if body["summary"] != nil {
		t.Errorf("summary = %v, want null before generation", body["summary"])
	}
	if s := strField(t, body, "state"); s != "pending" && s != "unavailable" {
		t.Errorf("state = %s, want pending or unavailable", s)
	}

	items, ok := body["failures"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("failure groups = %v, want one class", body["failures"])
	}
	group := items[0].(map[string]any)
	if n := numField(t, group, "count"); n != 4 {
		t.Errorf("count = %d, want 4", n)
	}
	if n := numField(t, group, "distinct_messages"); n != 2 {
		t.Errorf("distinct_messages = %d, want 2", n)
	}
}

func TestFailureSummaryCache(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, tok := actor(t, ctx, pool, tn.orgID, "viewer")
	base := server(t, pool)

	failJob(t, ctx, pool, tn, "connection refused")

	var digest string
	testdb.Must(t, pool.QueryRow(ctx, `select fl.failure_digest($1, $2)`,
		tn.queueID, jobs.FailureWindow).Scan(&digest))
	_, err := pool.Exec(ctx, `insert into failure_summaries (queue_id, digest, summary, model)
		values ($1, $2, 'the database is refusing connections', 'test-model')`, tn.queueID, digest)
	testdb.Must(t, err)

	_, _, raw := do(t, base, tok, "GET", "/queues/"+tn.queueID.String()+"/failure-summary", nil, nil)
	body := asMap(t, raw)
	if s := strField(t, body, "state"); s != "current" {
		t.Fatalf("state = %s, want current", s)
	}
	if s := strField(t, body, "summary"); s != "the database is refusing connections" {
		t.Errorf("summary = %q", s)
	}

	failJob(t, ctx, pool, tn, "disk full")

	_, _, raw = do(t, base, tok, "GET", "/queues/"+tn.queueID.String()+"/failure-summary", nil, nil)
	body = asMap(t, raw)
	if s := strField(t, body, "state"); s != "stale" {
		t.Errorf("state = %s, want stale after new failures", s)
	}
}
