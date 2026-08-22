package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestIdempotency(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	tn := setup(t, ctx, pool)
	_, tok := actor(t, ctx, pool, tn.orgID, "member")
	base := server(t, pool)
	path := "/queues/" + tn.queueID.String() + "/jobs"

	key := map[string]string{"Idempotency-Key": "k1"}
	first := map[string]any{"type": "noop", "payload": map[string]any{"a": 1}}
	second := map[string]any{"type": "noop", "payload": map[string]any{"a": 2}}

	st, _, raw := do(t, base, tok, "POST", path, first, key)
	if st != 201 {
		t.Fatalf("first submit = %d, want 201", st)
	}
	id1 := strField(t, asMap(t, raw), "id")

	st, _, raw = do(t, base, tok, "POST", path, first, key)
	if st != 200 {
		t.Fatalf("replayed submit = %d, want 200", st)
	}
	if id2 := strField(t, asMap(t, raw), "id"); id2 != id1 {
		t.Fatalf("replayed id = %s, want %s", id2, id1)
	}

	if st, _, _ = do(t, base, tok, "POST", path, second, key); st != 409 {
		t.Fatalf("changed payload = %d, want 409", st)
	}

	testdb.Advance(t, pool, 25*time.Hour)

	st, _, raw = do(t, base, tok, "POST", path, first, key)
	if st != 201 {
		t.Fatalf("post expiry submit = %d, want 201", st)
	}
	if id3 := strField(t, asMap(t, raw), "id"); id3 == id1 {
		t.Fatalf("post expiry id = %s, want a fresh id", id3)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
