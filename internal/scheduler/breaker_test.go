package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func breakerState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID) string {
	t.Helper()
	var s string
	testdb.Must(t, pool.QueryRow(ctx, `select breaker_state::text from queues where id = $1`, queueID).Scan(&s))
	return s
}

func breakerEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, topic string, queueID uuid.UUID) int {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from events where topic = $1 and entity_id = $2`, topic, queueID).Scan(&n))
	return n
}

func halfOpen(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID, openUntil time.Time) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`update queues set breaker_state = 'half_open', breaker_open_until = $2, breaker_probe_budget = 1 where id = $1`,
		queueID, openUntil)
	testdb.Must(t, err)
}

func TestBreakerOpens(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	for i := 0; i < 3; i++ {
		testdb.NewExecution(t, ctx, pool, queueID, workerID, "timeout", testdb.Epoch.Add(-time.Duration(i)*time.Second))
	}

	changed, err := scheduler.Breaker(ctx, pool, 5, 3, 30*time.Second)
	testdb.Must(t, err)
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	if got := breakerState(t, ctx, pool, queueID); got != "open" {
		t.Errorf("state = %q, want open", got)
	}

	var openUntil *time.Time
	var budget int
	testdb.Must(t, pool.QueryRow(ctx,
		`select breaker_open_until, breaker_probe_budget from queues where id = $1`, queueID).Scan(&openUntil, &budget))
	if openUntil == nil {
		t.Error("breaker_open_until is null, want set")
	}
	if budget != 0 {
		t.Errorf("probe budget = %d, want 0", budget)
	}
	if got := breakerEvents(t, ctx, pool, "queue.breaker_opened", queueID); got != 1 {
		t.Errorf("breaker_opened events = %d, want 1", got)
	}
}

func TestBreakerBelowTrip(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	for i := 0; i < 2; i++ {
		testdb.NewExecution(t, ctx, pool, queueID, workerID, "timeout", testdb.Epoch.Add(-time.Duration(i)*time.Second))
	}

	changed, err := scheduler.Breaker(ctx, pool, 5, 3, 30*time.Second)
	testdb.Must(t, err)
	if changed != 0 {
		t.Fatalf("changed = %d, want 0", changed)
	}
	if got := breakerState(t, ctx, pool, queueID); got != "closed" {
		t.Errorf("state = %q, want closed", got)
	}
	if got := breakerEvents(t, ctx, pool, "queue.breaker_opened", queueID); got != 0 {
		t.Errorf("breaker_opened events = %d, want 0", got)
	}
}

func TestBreakerIgnoresFenced(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	outcomes := []string{"fenced", "fenced", "fenced", "cancelled", "cancelled"}
	for i, o := range outcomes {
		testdb.NewExecution(t, ctx, pool, queueID, workerID, o, testdb.Epoch.Add(-time.Duration(i)*time.Second))
	}

	changed, err := scheduler.Breaker(ctx, pool, 5, 3, 30*time.Second)
	testdb.Must(t, err)
	if changed != 0 {
		t.Fatalf("changed = %d, want 0", changed)
	}
	if got := breakerState(t, ctx, pool, queueID); got != "closed" {
		t.Errorf("state = %q, want closed", got)
	}
}

func TestBreakerHalfOpenCloses(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	halfOpen(t, ctx, pool, queueID, testdb.Epoch)
	testdb.NewExecution(t, ctx, pool, queueID, workerID, "success", testdb.Epoch.Add(time.Minute))

	changed, err := scheduler.Breaker(ctx, pool, 5, 3, 30*time.Second)
	testdb.Must(t, err)
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	if got := breakerState(t, ctx, pool, queueID); got != "closed" {
		t.Errorf("state = %q, want closed", got)
	}

	var openUntil *time.Time
	testdb.Must(t, pool.QueryRow(ctx,
		`select breaker_open_until from queues where id = $1`, queueID).Scan(&openUntil))
	if openUntil != nil {
		t.Errorf("breaker_open_until = %s, want null", openUntil)
	}
	if got := breakerEvents(t, ctx, pool, "queue.breaker_closed", queueID); got != 1 {
		t.Errorf("breaker_closed events = %d, want 1", got)
	}
}

func TestBreakerHalfOpenReopens(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	halfOpen(t, ctx, pool, queueID, testdb.Epoch)
	testdb.NewExecution(t, ctx, pool, queueID, workerID, "timeout", testdb.Epoch.Add(time.Minute))

	changed, err := scheduler.Breaker(ctx, pool, 5, 3, 30*time.Second)
	testdb.Must(t, err)
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	if got := breakerState(t, ctx, pool, queueID); got != "open" {
		t.Errorf("state = %q, want open", got)
	}
	if got := breakerEvents(t, ctx, pool, "queue.breaker_reopened", queueID); got != 1 {
		t.Errorf("breaker_reopened events = %d, want 1", got)
	}
}

func TestBreakerHalfOpenNoProbe(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	workerID := testdb.NewWorker(t, ctx, pool, projectID)

	halfOpen(t, ctx, pool, queueID, testdb.Epoch)
	testdb.NewExecution(t, ctx, pool, queueID, workerID, "success", testdb.Epoch.Add(-time.Minute))

	changed, err := scheduler.Breaker(ctx, pool, 5, 3, 30*time.Second)
	testdb.Must(t, err)
	if changed != 0 {
		t.Fatalf("changed = %d, want 0", changed)
	}
	if got := breakerState(t, ctx, pool, queueID); got != "half_open" {
		t.Errorf("state = %q, want half_open", got)
	}
	if got := breakerEvents(t, ctx, pool, "queue.breaker_closed", queueID); got != 0 {
		t.Errorf("breaker_closed events = %d, want 0", got)
	}
	if got := breakerEvents(t, ctx, pool, "queue.breaker_reopened", queueID); got != 0 {
		t.Errorf("breaker_reopened events = %d, want 0", got)
	}
}
