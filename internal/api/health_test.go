package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/testdb"
	"github.com/shrutu0929/fenceline/internal/worker"
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
	_, err = pool.Exec(ctx, `update queues set
		breaker_state = 'open', breaker_open_until = fl.now() + interval '30 seconds' where id = $1`,
		ten.queueID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `update queue_shards set in_flight = fl.shard_slots($1, shard)
		where queue_id = $1`, ten.queueID)
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

func TestQueueSeries(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	_, err := pool.Exec(ctx, `insert into queue_stats_minute
		(queue_id, minute, completed, failed, duration_ms_p95)
		values ($1, date_trunc('minute', fl.now()) - interval '2 minutes', 5, 1, 900),
		       ($1, date_trunc('minute', fl.now()) - interval '1 minute', 7, 0, 800),
		       ($1, date_trunc('minute', fl.now()) - interval '3 hours', 99, 0, 100)`, ten.queueID)
	testdb.Must(t, err)

	code, _, body := do(t, base, token, "GET", "/stats/queues/"+ten.queueID.String()+"/series", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	items := asMap(t, body)["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	if n := numField(t, items[0].(map[string]any), "completed"); n != 5 {
		t.Errorf("first minute completed = %d, want 5", n)
	}

	code, _, body = do(t, base, token, "GET", "/stats/queues/"+ten.queueID.String()+"/series?minutes=0", nil, nil)
	if code != http.StatusBadRequest {
		t.Errorf("status for minutes=0 = %d, want 400: %s", code, body)
	}
}

func TestProjectHealth(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	other := testdb.NewQueue(t, ctx, pool, ten.projectID, ten.policyID, 2)
	_, err := pool.Exec(ctx, `update queues set name = 'zulu' where id = $1`, other)
	testdb.Must(t, err)

	_, err = pool.Exec(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, priority, run_at)
		values ($1, $2, 'noop', $3, 'queued', 2, fl.now())`, ten.projectID, ten.queueID, ten.policyID)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `update queues set paused = true where id = $1`, other)
	testdb.Must(t, err)
	_, err = pool.Exec(ctx, `update queue_shards set in_flight = fl.shard_slots($1, shard)
		where queue_id = $1`, other)
	testdb.Must(t, err)

	code, _, body := do(t, base, token, "GET", "/projects/"+ten.projectID.String()+"/queue-health", nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	items := asMap(t, body)["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}

	byName := map[string]map[string]any{}
	for _, it := range items {
		h := it.(map[string]any)
		byName[strField(t, h["queue"].(map[string]any), "name")] = h
	}

	zulu := byName["zulu"]
	if zulu["saturated"] != true {
		t.Errorf("zulu saturated = %v, want true", zulu["saturated"])
	}
	if q := zulu["queue"].(map[string]any); q["paused"] != true {
		t.Errorf("zulu paused = %v, want true", q["paused"])
	}

	var withTier map[string]any
	for _, h := range byName {
		if len(h["tiers"].([]any)) > 0 {
			withTier = h
		}
	}
	if withTier == nil {
		t.Fatal("queues with a tier = 0, want 1")
	}
	tier := withTier["tiers"].([]any)[0].(map[string]any)
	if p := numField(t, tier, "priority"); p != 2 {
		t.Errorf("tier priority = %d, want 2", p)
	}
	if numField(t, withTier, "live_workers") != 0 {
		t.Errorf("live workers = %v, want 0", withTier["live_workers"])
	}
}

func TestQueueHealthLiveWorkers(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	code, _, body := do(t, base, token, "GET", "/stats/queues/"+ten.queueID.String(), nil, nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	if n := numField(t, asMap(t, body), "live_workers"); n != 0 {
		t.Fatalf("live workers before registering = %d, want 0", n)
	}

	workerID, err := worker.Register(ctx, pool, ten.queueID, 4)
	testdb.Must(t, err)
	cfg := worker.DefaultConfig()
	cfg.QueueID = ten.queueID
	cfg.WorkerID = workerID
	cfg.Drain = 200 * time.Millisecond
	cfg.Poll = 20 * time.Millisecond

	runCtx, stop := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		worker.Run(runCtx, pool, cfg, map[string]worker.Handler{
			"noop": func(ctx context.Context, j worker.Job) error { return nil },
		})
	}()

	live := func() int {
		_, _, raw := do(t, base, token, "GET", "/stats/queues/"+ten.queueID.String(), nil, nil)
		return numField(t, asMap(t, raw), "live_workers")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && live() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := live(); got != 1 {
		t.Errorf("live workers = %d, want 1", got)
	}

	stop()
	<-finished
}
