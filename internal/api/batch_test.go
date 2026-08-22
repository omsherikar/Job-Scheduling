package api_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func jobCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID) int {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from jobs where queue_id = $1`, queueID).Scan(&n))
	return n
}

func TestBatchAtomic(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, tok := actor(t, ctx, pool, tn.orgID, "member")
	base := server(t, pool)

	t.Run("valid batch", func(t *testing.T) {
		body := map[string]any{"jobs": []map[string]any{
			{"type": "noop"}, {"type": "noop"}, {"type": "noop"},
		}}
		st, _, raw := do(t, base, tok, "POST", "/queues/"+tn.queueID.String()+"/jobs/batch", body, nil)
		if st != 201 {
			t.Fatalf("batch = %d, want 201", st)
		}
		if c := numField(t, asMap(t, raw), "count"); c != 3 {
			t.Fatalf("count = %d, want 3", c)
		}
		if n := jobCount(t, ctx, pool, tn.queueID); n != 3 {
			t.Fatalf("jobs = %d, want 3", n)
		}
	})

	t.Run("invalid batch", func(t *testing.T) {
		queueID := testdb.NewQueue(t, ctx, pool, tn.projectID, tn.policyID, 10)
		body := map[string]any{"jobs": []map[string]any{
			{"type": "noop"},
			{"type": "noop", "timeout_ms": 0},
			{"type": "noop"},
		}}
		st, hdr, raw := do(t, base, tok, "POST", "/queues/"+queueID.String()+"/jobs/batch", body, nil)
		if st == 201 {
			t.Fatalf("batch = %d, want an error", st)
		}
		if ct := hdr.Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("content-type = %q, want application/problem+json", ct)
		}
		if rid := strField(t, asMap(t, raw), "request_id"); rid == "" {
			t.Fatal("request_id = empty, want non-empty")
		}
		if n := jobCount(t, ctx, pool, queueID); n != 0 {
			t.Fatalf("jobs = %d, want 0", n)
		}
	})

	testdb.CheckInvariants(t, ctx, pool)
}
