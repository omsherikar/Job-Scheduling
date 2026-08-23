package api_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestEventReplay(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	for i := 0; i < 5; i++ {
		_, err := pool.Exec(ctx, `select fl.emit($1, $2, $2, '{}'::jsonb)`,
			fmt.Sprintf("t%d", i), ten.projectID)
		testdb.Must(t, err)
	}

	path := "/projects/" + ten.projectID.String() + "/events"
	code, _, body := do(t, base, token, "GET", path+"?limit=2", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	page := asMap(t, body)
	items, ok := page["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("items = %v, want 2", page["items"])
	}
	if strField(t, items[0].(map[string]any), "topic") != "t0" {
		t.Errorf("first topic = %v, want t0", items[0])
	}

	next := numField(t, page, "next")
	code, _, body = do(t, base, token, "GET", fmt.Sprintf("%s?after=%d&limit=10", path, next), nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	rest := asMap(t, body)["items"].([]any)
	if len(rest) != 3 {
		t.Fatalf("rest = %d, want 3", len(rest))
	}
	if strField(t, rest[0].(map[string]any), "topic") != "t2" {
		t.Errorf("resumed at %v, want t2", rest[0])
	}
}

func TestEventCursorTooOld(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	_, err := pool.Exec(ctx, `select fl.emit('a', $1, $1, '{}'::jsonb)`, ten.projectID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `update events_retention set low_water_id = 50`)
	testdb.Must(t, err)

	path := "/projects/" + ten.projectID.String() + "/events"
	code, _, body := do(t, base, token, "GET", path+"?after=3", nil, nil)
	if code != http.StatusGone {
		t.Fatalf("status = %d, want 410: %s", code, body)
	}
	if got := strField(t, asMap(t, body), "title"); got != "cursor too old" {
		t.Errorf("title = %q, want cursor too old", got)
	}

	code, _, _ = do(t, base, token, "GET", path+"?after=49", nil, nil)
	if code != http.StatusOK {
		t.Errorf("status at the low water = %d, want 200", code)
	}
}

func TestEventLimitCap(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	for i := 0; i < 105; i++ {
		_, err := pool.Exec(ctx, `select fl.emit('t', $1, $1, '{}'::jsonb)`, ten.projectID)
		testdb.Must(t, err)
	}

	path := "/projects/" + ten.projectID.String() + "/events"
	code, _, body := do(t, base, token, "GET", path+"?limit=1000", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	items := asMap(t, body)["items"].([]any)
	if len(items) != 100 {
		t.Errorf("items = %d, want 100", len(items))
	}
}

func TestEventProjectIsolation(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	mine := setup(t, ctx, pool)
	theirs := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, mine.orgID, "viewer")
	base := server(t, pool)

	_, err := pool.Exec(ctx, `select fl.emit('theirs', $1, $1, '{}'::jsonb)`, theirs.projectID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `select fl.emit('mine', $1, $1, '{}'::jsonb)`, mine.projectID)
	testdb.Must(t, err)

	code, _, body := do(t, base, token, "GET", "/projects/"+mine.projectID.String()+"/events", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	items := asMap(t, body)["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if got := strField(t, items[0].(map[string]any), "topic"); got != "mine" {
		t.Errorf("topic = %q, want mine", got)
	}

	code, _, _ = do(t, base, token, "GET", "/projects/"+theirs.projectID.String()+"/events", nil, nil)
	if code != http.StatusNotFound {
		t.Errorf("status on another project = %d, want 404", code)
	}
}

func TestEventFromMutation(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "member")
	base := server(t, pool)

	jobID := testdb.NewJob(t, ctx, pool, ten.projectID, ten.queueID, ten.policyID)
	code, _, body := do(t, base, token, "POST", "/jobs/"+jobID.String()+"/cancel", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}

	code, _, body = do(t, base, token, "GET", "/projects/"+ten.projectID.String()+"/events", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	items := asMap(t, body)["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	first := items[0].(map[string]any)
	if got := strField(t, first, "topic"); got != "job.cancelled" {
		t.Errorf("topic = %q, want job.cancelled", got)
	}
	if got := strField(t, first, "entity_id"); got != jobID.String() {
		t.Errorf("entity_id = %q, want %s", got, jobID)
	}
}
