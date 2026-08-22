package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestDepthCap(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, err := pool.Exec(ctx, `update queues set max_depth = 2 where id = $1`, tn.queueID)
	testdb.Must(t, err)
	_, tok := actor(t, ctx, pool, tn.orgID, "member")
	base := server(t, pool)
	path := "/queues/" + tn.queueID.String() + "/jobs"
	job := map[string]any{"type": "noop"}

	for i := 0; i < 2; i++ {
		if st, _, _ := do(t, base, tok, "POST", path, job, nil); st != 201 {
			t.Fatalf("submit %d = %d, want 201", i, st)
		}
	}

	st, hdr, raw := do(t, base, tok, "POST", path, job, nil)
	if st != 429 {
		t.Fatalf("over cap = %d, want 429", st)
	}
	if ra := hdr.Get("Retry-After"); ra != "1" {
		t.Fatalf("Retry-After = %q, want 1", ra)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
	if rid := strField(t, asMap(t, raw), "request_id"); rid == "" {
		t.Fatal("request_id = empty, want non-empty")
	}

	worker := testdb.NewWorker(t, ctx, pool, tn.projectID)
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: tn.queueID, WorkerID: worker, FreeSlots: 2, Lease: 30 * time.Second})
	testdb.Must(t, err)
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2", len(claimed))
	}

	if st, _, _ := do(t, base, tok, "POST", path, job, nil); st != 201 {
		t.Fatalf("submit after drain = %d, want 201", st)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
