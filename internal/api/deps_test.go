package api_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func submit(t *testing.T, base, token string, queueID uuid.UUID, typ string, parents ...string) (uuid.UUID, string) {
	t.Helper()
	body := map[string]any{"type": typ}
	if len(parents) > 0 {
		body["depends_on"] = parents
	}
	code, _, raw := do(t, base, token, "POST", "/queues/"+queueID.String()+"/jobs", body, nil)
	if code != http.StatusCreated {
		t.Fatalf("submit status = %d, want 201: %s", code, raw)
	}
	m := asMap(t, raw)
	id, err := uuid.Parse(strField(t, m, "id"))
	testdb.Must(t, err)
	return id, strField(t, m, "status")
}

func finish(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ten tenant, jobID uuid.UUID, fail error) {
	t.Helper()
	worker := testdb.NewWorker(t, ctx, pool, ten.projectID)
	claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
		QueueID: ten.queueID, WorkerID: worker, FreeSlots: 10, Lease: time.Hour})
	testdb.Must(t, err)
	for _, c := range claimed {
		if c.ID != jobID {
			continue
		}
		exec, err := jobs.Start(ctx, pool, c.ID, c.Fence, worker)
		testdb.Must(t, err)
		if fail != nil {
			testdb.Must(t, jobs.Fail(ctx, pool, c.ID, c.Fence, fail))
		} else {
			testdb.Must(t, jobs.Complete(ctx, pool, c.ID, c.Fence, exec.ID))
		}
		return
	}
	t.Fatalf("job %s was not claimable", jobID)
}

func TestDependencyGate(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	parent, _ := submit(t, base, token, ten.queueID, "build")
	child, status := submit(t, base, token, ten.queueID, "deploy", parent.String())
	if status != "scheduled" {
		t.Fatalf("child status = %q, want scheduled", status)
	}

	var pending int
	testdb.Must(t, pool.QueryRow(ctx, `select pending_deps from jobs where id = $1`, child).Scan(&pending))
	if pending != 1 {
		t.Fatalf("pending_deps = %d, want 1", pending)
	}

	finish(t, ctx, pool, ten, parent, nil)

	if s := testdb.JobStatus(t, ctx, pool, child); s != "queued" {
		t.Errorf("child status = %q, want queued", s)
	}
	testdb.Must(t, pool.QueryRow(ctx, `select pending_deps from jobs where id = $1`, child).Scan(&pending))
	if pending != 0 {
		t.Errorf("pending_deps = %d, want 0", pending)
	}
}

func TestDependencyDeadLetterCancels(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	parent, _ := submit(t, base, token, ten.queueID, "build")
	child, _ := submit(t, base, token, ten.queueID, "deploy", parent.String())
	grand, _ := submit(t, base, token, ten.queueID, "notify", child.String())

	finish(t, ctx, pool, ten, parent, jobs.ErrPermanent)

	if s := testdb.JobStatus(t, ctx, pool, parent); s != "dead_letter" {
		t.Fatalf("parent status = %q, want dead_letter", s)
	}
	for name, id := range map[string]uuid.UUID{"child": child, "grandchild": grand} {
		if s := testdb.JobStatus(t, ctx, pool, id); s != "cancelled" {
			t.Errorf("%s status = %q, want cancelled", name, s)
		}
	}

	var pending int
	testdb.Must(t, pool.QueryRow(ctx, `select pending_deps from jobs where id = $1`, grand).Scan(&pending))
	if pending != 0 {
		t.Errorf("grandchild pending_deps = %d, want 0", pending)
	}
}

func TestDependencyReplayRefused(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	parent, _ := submit(t, base, token, ten.queueID, "build")
	child, _ := submit(t, base, token, ten.queueID, "deploy", parent.String())
	finish(t, ctx, pool, ten, parent, jobs.ErrPermanent)

	code, _, raw := do(t, base, token, "POST", "/dlq/"+parent.String()+"/replay", nil, nil)
	if code != http.StatusConflict {
		t.Fatalf("replay status = %d, want 409: %s", code, raw)
	}
	if got := strField(t, asMap(t, raw), "detail"); !strings.Contains(got, child.String()) {
		t.Errorf("detail = %q, want it to name %s", got, child)
	}
	if s := testdb.JobStatus(t, ctx, pool, child); s != "cancelled" {
		t.Errorf("child status = %q, want cancelled", s)
	}
}

func TestDependencyCycleRefused(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	a, _ := submit(t, base, token, ten.queueID, "a")
	b, _ := submit(t, base, token, ten.queueID, "b", a.String())

	_, err := pool.Exec(ctx, `insert into job_dependencies (parent_id, child_id) values ($1, $2)`, b, a)
	testdb.Must(t, err)

	code, _, raw := do(t, base, token, "POST", "/queues/"+ten.queueID.String()+"/jobs",
		map[string]any{"type": "c", "depends_on": []string{a.String()}}, nil)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 with the cycle already present: %s", code, raw)
	}

	var n int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from job_dependencies where parent_id = $1 and child_id = $2`, a, b).Scan(&n))
	if n != 1 {
		t.Errorf("dependency rows = %d, want 1", n)
	}
}

func deadBatch(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ten tenant, n int) []uuid.UUID {
	t.Helper()
	worker := testdb.NewWorker(t, ctx, pool, ten.projectID)
	var out []uuid.UUID
	for i := 0; i < n; i++ {
		id := testdb.NewJob(t, ctx, pool, ten.projectID, ten.queueID, ten.policyID)
		claimed, err := jobs.Claim(ctx, pool, jobs.ClaimRequest{
			QueueID: ten.queueID, WorkerID: worker, FreeSlots: 1, Lease: time.Hour})
		testdb.Must(t, err)
		_, err = jobs.Start(ctx, pool, id, claimed[0].Fence, worker)
		testdb.Must(t, err)
		testdb.Must(t, jobs.Fail(ctx, pool, id, claimed[0].Fence, jobs.ErrPermanent))
		out = append(out, id)
	}
	return out
}

func TestBulkReplayRate(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	dead := deadBatch(t, ctx, pool, ten, 5)

	code, _, raw := do(t, base, token, "POST", "/queues/"+ten.queueID.String()+"/dlq/replay",
		map[string]any{"limit": 10, "rate_per_sec": 2}, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, raw)
	}
	if n := numField(t, asMap(t, raw), "replayed"); n != 5 {
		t.Fatalf("replayed = %d, want 5", n)
	}

	for _, id := range dead {
		if s := testdb.JobStatus(t, ctx, pool, id); s != "queued" {
			t.Errorf("status = %q, want queued", s)
		}
	}

	var spread float64
	testdb.Must(t, pool.QueryRow(ctx,
		`select extract(epoch from (max(run_at) - min(run_at))) from jobs where id = any($1)`,
		dead).Scan(&spread))
	if spread < 1.9 || spread > 2.1 {
		t.Errorf("run_at spread = %.2fs, want ~2s", spread)
	}
}

func TestBulkReplayDepthCap(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	deadBatch(t, ctx, pool, ten, 5)
	_, err := pool.Exec(ctx, `update queues set max_depth = 2 where id = $1`, ten.queueID)
	testdb.Must(t, err)

	code, _, raw := do(t, base, token, "POST", "/queues/"+ten.queueID.String()+"/dlq/replay",
		map[string]any{"limit": 100}, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, raw)
	}
	if n := numField(t, asMap(t, raw), "replayed"); n != 2 {
		t.Errorf("replayed = %d, want 2", n)
	}
}

func TestBulkReplaySkipsCancelledKin(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	parent, _ := submit(t, base, token, ten.queueID, "build")
	submit(t, base, token, ten.queueID, "deploy", parent.String())
	finish(t, ctx, pool, ten, parent, jobs.ErrPermanent)

	code, _, raw := do(t, base, token, "POST", "/queues/"+ten.queueID.String()+"/dlq/replay",
		map[string]any{"limit": 100}, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, raw)
	}
	if n := numField(t, asMap(t, raw), "replayed"); n != 0 {
		t.Errorf("replayed = %d, want 0", n)
	}
	if s := testdb.JobStatus(t, ctx, pool, parent); s != "dead_letter" {
		t.Errorf("parent status = %q, want dead_letter", s)
	}
}

func TestJobKin(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	parent, _ := submit(t, base, token, ten.queueID, "build")
	child, _ := submit(t, base, token, ten.queueID, "deploy", parent.String())

	code, _, raw := do(t, base, token, "GET", "/jobs/"+child.String(), nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, raw)
	}
	parents := asMap(t, raw)["parents"].([]any)
	if len(parents) != 1 {
		t.Fatalf("parents = %d, want 1", len(parents))
	}
	if got := strField(t, parents[0].(map[string]any), "type"); got != "build" {
		t.Errorf("parent type = %q, want build", got)
	}

	code, _, raw = do(t, base, token, "GET", "/jobs/"+parent.String(), nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, raw)
	}
	children := asMap(t, raw)["children"].([]any)
	if len(children) != 1 {
		t.Fatalf("children = %d, want 1", len(children))
	}
	if got := strField(t, children[0].(map[string]any), "status"); got != "scheduled" {
		t.Errorf("child status = %q, want scheduled", got)
	}
}
