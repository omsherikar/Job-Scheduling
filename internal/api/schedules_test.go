package api_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestScheduleLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "admin")
	base := server(t, pool)

	code, _, raw := do(t, base, token, "POST", "/queues/"+ten.queueID.String()+"/schedules",
		map[string]any{
			"name":      "nightly",
			"cron_expr": "0 3 * * *",
			"timezone":  "America/New_York",
			"job_type":  "report",
			"payload":   map[string]any{"scope": "all"},
		}, nil)
	if code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201: %s", code, raw)
	}
	created := asMap(t, raw)
	id := strField(t, created, "id")
	if got := strField(t, created, "overlap_policy"); got != "skip" {
		t.Errorf("overlap_policy = %q, want skip", got)
	}
	if created["enabled"] != true {
		t.Errorf("enabled = %v, want true", created["enabled"])
	}

	next, err := time.Parse(time.RFC3339, strField(t, created, "next_run_at"))
	testdb.Must(t, err)
	york, err := time.LoadLocation("America/New_York")
	testdb.Must(t, err)
	at := next.In(york)
	if at.Hour() != 3 || at.Minute() != 0 {
		t.Errorf("next run local time = %02d:%02d, want 03:00", at.Hour(), at.Minute())
	}

	code, _, raw = do(t, base, token, "GET", "/queues/"+ten.queueID.String()+"/schedules", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("list status = %d, want 200: %s", code, raw)
	}
	if items := asMap(t, raw)["items"].([]any); len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}

	code, _, raw = do(t, base, token, "PATCH", "/schedules/"+id,
		map[string]any{"cron_expr": "*/5 * * * *", "catchup_policy": "fire_once"}, nil)
	if code != http.StatusOK {
		t.Fatalf("patch status = %d, want 200: %s", code, raw)
	}
	patched := asMap(t, raw)
	if got := strField(t, patched, "cron_expr"); got != "*/5 * * * *" {
		t.Errorf("cron_expr = %q, want */5 * * * *", got)
	}
	if got := strField(t, patched, "catchup_policy"); got != "fire_once" {
		t.Errorf("catchup_policy = %q, want fire_once", got)
	}

	code, _, raw = do(t, base, token, "POST", "/schedules/"+id+"/pause", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("pause status = %d, want 200: %s", code, raw)
	}
	if asMap(t, raw)["enabled"] != false {
		t.Errorf("enabled = %v, want false", asMap(t, raw)["enabled"])
	}

	code, _, _ = do(t, base, token, "POST", "/schedules/"+id+"/resume", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("resume status = %d, want 200", code)
	}

	code, _, _ = do(t, base, token, "DELETE", "/schedules/"+id, nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", code)
	}
	code, _, _ = do(t, base, token, "GET", "/schedules/"+id, nil, nil)
	if code != http.StatusNotFound {
		t.Errorf("get after delete status = %d, want 404", code)
	}
}

func TestScheduleValidation(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "admin")
	base := server(t, pool)

	path := "/queues/" + ten.queueID.String() + "/schedules"
	for _, tc := range []struct {
		name string
		body map[string]any
		want string
	}{
		{"bad cron", map[string]any{"name": "a", "cron_expr": "not a cron", "job_type": "t"}, "cron_expr"},
		{"bad timezone", map[string]any{"name": "b", "cron_expr": "* * * * *", "job_type": "t", "timezone": "Mars/Olympus"}, "timezone"},
		{"bad overlap", map[string]any{"name": "c", "cron_expr": "* * * * *", "job_type": "t", "overlap_policy": "queue"}, "overlap_policy"},
		{"bad catchup", map[string]any{"name": "d", "cron_expr": "* * * * *", "job_type": "t", "catchup_policy": "all"}, "catchup_policy"},
		{"no name", map[string]any{"cron_expr": "* * * * *", "job_type": "t"}, "required"},
	} {
		code, _, raw := do(t, base, token, "POST", path, tc.body, nil)
		if code != http.StatusBadRequest {
			t.Errorf("%s status = %d, want 400: %s", tc.name, code, raw)
			continue
		}
		if got := strField(t, asMap(t, raw), "detail"); !strings.Contains(got, tc.want) {
			t.Errorf("%s detail = %q, want substring %q", tc.name, got, tc.want)
		}
	}

	code, _, _ := do(t, base, token, "POST", path,
		map[string]any{"name": "dup", "cron_expr": "* * * * *", "job_type": "t"}, nil)
	if code != http.StatusCreated {
		t.Fatalf("first create status = %d, want 201", code)
	}
	code, _, _ = do(t, base, token, "POST", path,
		map[string]any{"name": "dup", "cron_expr": "* * * * *", "job_type": "t"}, nil)
	if code != http.StatusConflict {
		t.Errorf("duplicate name status = %d, want 409", code)
	}
}

func TestScheduleFires(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "admin")
	base := server(t, pool)

	code, _, raw := do(t, base, token, "POST", "/queues/"+ten.queueID.String()+"/schedules",
		map[string]any{"name": "every minute", "cron_expr": "* * * * *", "job_type": "tick"}, nil)
	if code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201: %s", code, raw)
	}
	id, err := uuid.Parse(strField(t, asMap(t, raw), "id"))
	testdb.Must(t, err)

	var next time.Time
	testdb.Must(t, pool.QueryRow(ctx, `select next_run_at from schedules where id = $1`, id).Scan(&next))
	testdb.SetNow(t, pool, next.Add(time.Second))

	n, err := scheduler.Materialize(ctx, pool, 10)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("materialized = %d, want 1", n)
	}

	var typ string
	var scheduledFor time.Time
	testdb.Must(t, pool.QueryRow(ctx,
		`select type, scheduled_for from jobs where schedule_id = $1`, id).Scan(&typ, &scheduledFor))
	if typ != "tick" {
		t.Errorf("job type = %q, want tick", typ)
	}
	if !scheduledFor.Equal(next) {
		t.Errorf("scheduled_for = %s, want %s", scheduledFor, next)
	}
}

func TestScheduleClock(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	past := testdb.Epoch.AddDate(-1, 0, 0).UTC()
	testdb.SetNow(t, pool, past)

	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "admin")
	base := server(t, pool)

	code, _, raw := do(t, base, token, "POST", "/queues/"+ten.queueID.String()+"/schedules",
		map[string]any{"name": "daily", "cron_expr": "0 3 * * *", "job_type": "report"}, nil)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", code, raw)
	}

	next, err := time.Parse(time.RFC3339, strField(t, asMap(t, raw), "next_run_at"))
	testdb.Must(t, err)
	if gap := next.Sub(past); gap < 0 || gap > 24*time.Hour {
		t.Errorf("next_run_at = %s, want within a day of %s", next, past)
	}
}
