package api_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestAuth(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	base := server(t, pool)

	email := uuid.NewString() + "@example.com"
	pw := "password123"
	reg := map[string]any{"email": email, "password": pw, "name": "Ada"}

	st, _, raw := do(t, base, "", "POST", "/auth/register", reg, nil)
	if st != 201 {
		t.Fatalf("register = %d, want 201", st)
	}
	if e := strField(t, asMap(t, raw), "email"); e != email {
		t.Fatalf("email = %s, want %s", e, email)
	}

	if st, _, _ := do(t, base, "", "POST", "/auth/register", reg, nil); st != 409 {
		t.Fatalf("duplicate register = %d, want 409", st)
	}

	bad := map[string]any{"email": email, "password": "wrong-password"}
	if st, _, _ := do(t, base, "", "POST", "/auth/login", bad, nil); st != 401 {
		t.Fatalf("bad password login = %d, want 401", st)
	}

	st, _, raw = do(t, base, "", "POST", "/auth/login", map[string]any{"email": email, "password": pw}, nil)
	if st != 200 {
		t.Fatalf("login = %d, want 200", st)
	}
	tok := strField(t, asMap(t, raw), "token")

	if st, _, _ := do(t, base, tok, "GET", "/orgs", nil, nil); st != 200 {
		t.Fatalf("authed read = %d, want 200", st)
	}

	if st, _, _ := do(t, base, tok, "POST", "/auth/logout", nil, nil); st != 204 {
		t.Fatalf("logout = %d, want 204", st)
	}
	if st, _, _ := do(t, base, tok, "GET", "/orgs", nil, nil); st != 401 {
		t.Fatalf("read after logout = %d, want 401", st)
	}

	if st, _, _ := do(t, base, "not-a-uuid", "GET", "/orgs", nil, nil); st != 401 {
		t.Fatalf("malformed token = %d, want 401", st)
	}
	if st, _, _ := do(t, base, uuid.NewString(), "GET", "/orgs", nil, nil); st != 401 {
		t.Fatalf("unknown token = %d, want 401", st)
	}

	st, _, raw = do(t, base, "", "POST", "/auth/login", map[string]any{"email": email, "password": pw}, nil)
	if st != 200 {
		t.Fatalf("relogin = %d, want 200", st)
	}
	expiring := strField(t, asMap(t, raw), "token")
	_, err := pool.Exec(ctx,
		`update sessions set expires_at = fl.now() - interval '1 hour' where id = $1`, expiring)
	testdb.Must(t, err)
	if st, _, _ := do(t, base, expiring, "GET", "/orgs", nil, nil); st != 401 {
		t.Fatalf("expired session = %d, want 401", st)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
