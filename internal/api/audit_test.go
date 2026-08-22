package api_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func hasAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, action string, actor, entityID uuid.UUID) {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from audit_log
		where project_id = $1 and action = $2 and actor_id = $3 and entity_id = $4`,
		projectID, action, actor, entityID).Scan(&n))
	if n != 1 {
		t.Errorf("audit rows for %s = %d, want 1", action, n)
	}
}

func auditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID) int {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from audit_log where project_id = $1`, projectID).Scan(&n))
	return n
}

func TestAuditTrail(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	_, err := pool.Exec(ctx, `update retry_policies set max_attempts = 1 where id = $1`, tn.policyID)
	testdb.Must(t, err)
	memberID, memberTok := actor(t, ctx, pool, tn.orgID, "member")
	adminID, adminTok := actor(t, ctx, pool, tn.orgID, "admin")
	base := server(t, pool)

	jobID := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)
	if st, _, _ := do(t, base, memberTok, "POST", "/jobs/"+jobID.String()+"/cancel", nil, nil); st != 200 {
		t.Fatalf("cancel = %d, want 200", st)
	}
	hasAudit(t, ctx, pool, tn.projectID, "job.cancel", memberID, jobID)

	body := map[string]any{"name": "audited-policy"}
	st, _, raw := do(t, base, adminTok, "POST", "/projects/"+tn.projectID.String()+"/retry-policies", body, nil)
	if st != 201 {
		t.Fatalf("create policy = %d, want 201", st)
	}
	policyID := uuid.MustParse(strField(t, asMap(t, raw), "id"))
	hasAudit(t, ctx, pool, tn.projectID, "policy.create", adminID, policyID)

	dead := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)
	deadLetter(t, ctx, pool, tn, dead)
	if st, _, _ := do(t, base, memberTok, "POST", "/dlq/"+dead.String()+"/replay", nil, nil); st != 200 {
		t.Fatalf("replay = %d, want 200", st)
	}
	hasAudit(t, ctx, pool, tn.projectID, "job.replay", memberID, dead)

	if st, _, _ := do(t, base, memberTok, "POST", "/queues/"+tn.queueID.String()+"/pause", nil, nil); st != 200 {
		t.Fatalf("pause = %d, want 200", st)
	}
	hasAudit(t, ctx, pool, tn.projectID, "queue.pause", memberID, tn.queueID)

	before := auditCount(t, ctx, pool, tn.projectID)
	do(t, base, memberTok, "GET", "/queues/"+tn.queueID.String(), nil, nil)
	do(t, base, memberTok, "GET", "/jobs?project="+tn.projectID.String(), nil, nil)
	if after := auditCount(t, ctx, pool, tn.projectID); after != before {
		t.Errorf("audit rows after reads = %d, want %d", after, before)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
