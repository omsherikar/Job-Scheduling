package api_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestOrgScope(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	base := server(t, pool)
	tn := setup(t, ctx, pool)

	_, adminTok := actor(t, ctx, pool, tn.orgID, "admin")
	_, viewerTok := actor(t, ctx, pool, tn.orgID, "viewer")

	if st, _, _ := do(t, base, adminTok, "GET", "/orgs/"+tn.orgID.String(), nil, nil); st != 200 {
		t.Errorf("admin get org = %d, want 200", st)
	}
	if st, _, _ := do(t, base, viewerTok, "GET", "/orgs/"+tn.orgID.String(), nil, nil); st != 200 {
		t.Errorf("viewer get org = %d, want 200", st)
	}
	if st, _, _ := do(t, base, viewerTok, "PATCH", "/orgs/"+tn.orgID.String(),
		map[string]any{"name": "renamed"}, nil); st != 403 {
		t.Errorf("viewer patch org = %d, want 403", st)
	}

	outsider := testdb.NewUser(t, ctx, pool)
	outsiderTok := testdb.NewSession(t, ctx, pool, outsider)
	if st, _, _ := do(t, base, outsiderTok, "GET", "/orgs/"+tn.orgID.String(), nil, nil); st != 404 {
		t.Errorf("outsider get org = %d, want 404", st)
	}

	newUser := testdb.NewUser(t, ctx, pool)
	var email string
	testdb.Must(t, pool.QueryRow(ctx, `select email from users where id = $1`, newUser).Scan(&email))
	if st, _, _ := do(t, base, adminTok, "POST", "/orgs/"+tn.orgID.String()+"/members",
		map[string]any{"email": email, "role": "member"}, nil); st != 201 {
		t.Errorf("add member = %d, want 201", st)
	}

	if st, _, _ := do(t, base, adminTok, "POST", "/orgs/"+uuid.NewString()+"/projects",
		map[string]any{"name": "nope"}, nil); st != 404 {
		t.Errorf("unknown org = %d, want 404", st)
	}
}
