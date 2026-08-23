package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestQueueHealthTiers(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	_, err := pool.Exec(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, priority, run_at)
		values ($1, $2, 'noop', $3, 'queued', 0, fl.now())`, ten.projectID, ten.queueID, ten.policyID)
	testdb.Must(t, err)
	testdb.Advance(t, pool, 5*time.Minute)
	_, err = pool.Exec(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, priority, run_at)
		values ($1, $2, 'noop', $3, 'queued', 3, fl.now())`, ten.projectID, ten.queueID, ten.policyID)
	testdb.Must(t, err)

	code, _, body := do(t, base, token, "GET", "/stats/queues/"+ten.queueID.String(), nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	stats := asMap(t, body)

	tiers, ok := stats["tiers"].([]any)
	if !ok || len(tiers) != 2 {
		t.Fatalf("tiers = %v, want 2", stats["tiers"])
	}
	high := tiers[0].(map[string]any)
	low := tiers[1].(map[string]any)
	if p := numField(t, high, "priority"); p != 3 {
		t.Errorf("first tier priority = %d, want 3", p)
	}
	if age := numField(t, high, "oldest_ready_seconds"); age != 0 {
		t.Errorf("priority 3 oldest = %d, want 0", age)
	}
	if p := numField(t, low, "priority"); p != 0 {
		t.Errorf("second tier priority = %d, want 0", p)
	}
	if age := numField(t, low, "oldest_ready_seconds"); age != 300 {
		t.Errorf("priority 0 oldest = %d, want 300", age)
	}
}

func TestQueueHealthStopped(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "admin")
	base := server(t, pool)

	code, _, body := do(t, base, token, "GET", "/stats/queues/"+ten.queueID.String(), nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	stats := asMap(t, body)
	if w := numField(t, stats, "live_workers"); w != 0 {
		t.Errorf("live workers = %d, want 0", w)
	}
	if stats["saturated"] != false {
		t.Errorf("saturated = %v, want false", stats["saturated"])
	}
	if stats["breaker_open_until"] != nil {
		t.Errorf("breaker_open_until = %v, want null", stats["breaker_open_until"])
	}

	workerID := testdb.NewWorker(t, ctx, pool, ten.projectID)
	_, err := pool.Exec(ctx, `insert into worker_queues (worker_id, queue_id) values ($1, $2)`,
		workerID, ten.queueID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `update queues set in_flight = max_concurrency,
		breaker_state = 'open', breaker_open_until = fl.now() + interval '30 seconds' where id = $1`,
		ten.queueID)
	testdb.Must(t, err)

	code, _, body = do(t, base, token, "GET", "/stats/queues/"+ten.queueID.String(), nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	stats = asMap(t, body)
	if w := numField(t, stats, "live_workers"); w != 1 {
		t.Errorf("live workers = %d, want 1", w)
	}
	if stats["saturated"] != true {
		t.Errorf("saturated = %v, want true", stats["saturated"])
	}
	if stats["breaker_open_until"] == nil {
		t.Error("breaker_open_until = null, want a timestamp")
	}
}
