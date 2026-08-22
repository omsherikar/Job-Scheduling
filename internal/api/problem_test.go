package api_test

import (
	"context"
	"testing"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestProblemJSON(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	base := server(t, pool)

	st, hdr, raw := do(t, base, "", "GET", "/orgs", nil, nil)
	if st != 401 {
		t.Fatalf("unauthorized = %d, want 401", st)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
	m := asMap(t, raw)
	if strField(t, m, "type") != "about:blank" {
		t.Fatalf("type = %v, want about:blank", m["type"])
	}
	if numField(t, m, "status") != 401 {
		t.Fatalf("status = %v, want 401", m["status"])
	}
	if strField(t, m, "title") == "" {
		t.Fatal("title = empty, want non-empty")
	}
	if strField(t, m, "request_id") == "" {
		t.Fatal("request_id = empty, want non-empty")
	}

	rid := "test-request-id-123"
	_, hdr, raw = do(t, base, "", "GET", "/orgs", nil, map[string]string{"X-Request-Id": rid})
	if got := hdr.Get("X-Request-Id"); got != rid {
		t.Fatalf("echoed request id = %q, want %q", got, rid)
	}
	if got := strField(t, asMap(t, raw), "request_id"); got != rid {
		t.Fatalf("body request_id = %q, want %q", got, rid)
	}

	st, hdr, raw = do(t, base, "", "POST", "/auth/register", rawBody("{not json"), nil)
	if st != 400 {
		t.Fatalf("bad json = %d, want 400", st)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
	if strField(t, asMap(t, raw), "request_id") == "" {
		t.Fatal("request_id = empty, want non-empty")
	}

	testdb.CheckInvariants(t, ctx, pool)
}
