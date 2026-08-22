package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func seedAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tn tenant, at time.Time) string {
	t.Helper()
	var id uuid.UUID
	testdb.Must(t, pool.QueryRow(ctx,
		`insert into jobs (project_id, queue_id, type, retry_policy_id, status, run_at, created_at)
		 values ($1, $2, 'noop', $3, 'queued', fl.now(), $4) returning id`,
		tn.projectID, tn.queueID, tn.policyID, at).Scan(&id))
	return id.String()
}

func TestKeysetPagination(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, tok := actor(t, ctx, pool, tn.orgID, "viewer")
	base := server(t, pool)

	want := map[string]bool{}
	for i := 0; i < 6; i++ {
		want[seedAt(t, ctx, pool, tn, testdb.Epoch.Add(time.Duration(i)*time.Minute))] = true
	}

	seen := []string{}
	cursor := ""
	inserted := false
	for {
		path := "/jobs?project=" + tn.projectID.String() + "&limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		st, _, raw := do(t, base, tok, "GET", path, nil, nil)
		if st != 200 {
			t.Fatalf("list = %d, want 200", st)
		}
		m := asMap(t, raw)
		items := m["items"].([]any)
		for _, it := range items {
			seen = append(seen, it.(map[string]any)["id"].(string))
		}
		cursor = strField(t, m, "next_cursor")
		if !inserted {
			seedAt(t, ctx, pool, tn, testdb.Epoch.Add(10*time.Minute))
			seedAt(t, ctx, pool, tn, testdb.Epoch.Add(11*time.Minute))
			inserted = true
		}
		if len(items) == 0 || cursor == "" {
			break
		}
	}

	if len(seen) != 6 {
		t.Fatalf("returned rows = %d, want 6", len(seen))
	}
	got := map[string]bool{}
	for _, id := range seen {
		if got[id] {
			t.Errorf("duplicate id %s", id)
		}
		got[id] = true
		if !want[id] {
			t.Errorf("unexpected id %s", id)
		}
	}
	for id := range want {
		if !got[id] {
			t.Errorf("missing id %s", id)
		}
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestPageSizeCap(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, tok := actor(t, ctx, pool, tn.orgID, "viewer")
	base := server(t, pool)

	_, err := pool.Exec(ctx,
		`insert into jobs (project_id, queue_id, type, retry_policy_id, status, run_at)
		 select $1, $2, 'noop', $3, 'queued', fl.now() from generate_series(1, 101)`,
		tn.projectID, tn.queueID, tn.policyID)
	testdb.Must(t, err)

	st, _, raw := do(t, base, tok, "GET", "/jobs?project="+tn.projectID.String()+"&limit=100000", nil, nil)
	if st != 200 {
		t.Fatalf("list = %d, want 200", st)
	}
	items := asMap(t, raw)["items"].([]any)
	if len(items) != 100 {
		t.Fatalf("page size = %d, want 100", len(items))
	}

	testdb.CheckInvariants(t, ctx, pool)
}
