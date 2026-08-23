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
