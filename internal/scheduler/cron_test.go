package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestMaterializeOnTick(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	scheduleID := testdb.NewSchedule(t, ctx, pool, queueID, "* * * * *", "UTC", "allow", "skip", testdb.Epoch)

	n, err := scheduler.Materialize(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("materialized = %d, want 1", n)
	}

	var jobs int
	var status string
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*), coalesce(min(status::text), '') from jobs where schedule_id = $1 and scheduled_for = $2`,
		scheduleID, testdb.Epoch).Scan(&jobs, &status))
	if jobs != 1 {
		t.Fatalf("jobs for tick = %d, want 1", jobs)
	}
	if status != "queued" {
		t.Errorf("status = %q, want queued", status)
	}

	var nextRun time.Time
	testdb.Must(t, pool.QueryRow(ctx, `select next_run_at from schedules where id = $1`, scheduleID).Scan(&nextRun))
	if !nextRun.After(testdb.Epoch) {
		t.Errorf("next_run_at = %s, want after %s", nextRun, testdb.Epoch)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestMaterializeDedup(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	scheduleID := testdb.NewSchedule(t, ctx, pool, queueID, "* * * * *", "UTC", "allow", "skip", testdb.Epoch)

	var mu sync.Mutex
	var fatal []error
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := scheduler.Materialize(ctx, pool, 100); err != nil {
				mu.Lock()
				fatal = append(fatal, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(fatal) != 0 {
		t.Fatalf("errors = %v, want none", fatal)
	}

	var jobs int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from jobs where schedule_id = $1`, scheduleID).Scan(&jobs))
	if jobs != 1 {
		t.Fatalf("jobs for schedule = %d, want 1", jobs)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestCatchupSkip(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	scheduleID := testdb.NewSchedule(t, ctx, pool, queueID, "* * * * *", "UTC", "allow", "skip", testdb.Epoch)

	now := testdb.Advance(t, pool, 6*time.Hour)
	n, err := scheduler.Materialize(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("materialized = %d, want 0", n)
	}

	var jobs int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from jobs where schedule_id = $1`, scheduleID).Scan(&jobs))
	if jobs != 0 {
		t.Fatalf("jobs for schedule = %d, want 0", jobs)
	}

	var skipped int
	testdb.Must(t, pool.QueryRow(ctx,
		`select (payload->>'count')::int from events where topic = 'schedule.ticks_skipped' and entity_id = $1`,
		scheduleID).Scan(&skipped))
	if skipped != 361 {
		t.Errorf("ticks skipped = %d, want 361", skipped)
	}

	var nextRun time.Time
	testdb.Must(t, pool.QueryRow(ctx, `select next_run_at from schedules where id = $1`, scheduleID).Scan(&nextRun))
	if !nextRun.After(now) {
		t.Errorf("next_run_at = %s, want after %s", nextRun, now)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestCatchupFireOnce(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	scheduleID := testdb.NewSchedule(t, ctx, pool, queueID, "* * * * *", "UTC", "allow", "fire_once", testdb.Epoch)

	testdb.Advance(t, pool, 6*time.Hour)
	n, err := scheduler.Materialize(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("materialized = %d, want 1", n)
	}

	var jobs int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from jobs where schedule_id = $1`, scheduleID).Scan(&jobs))
	if jobs != 1 {
		t.Fatalf("jobs for schedule = %d, want 1", jobs)
	}

	var skipped int
	testdb.Must(t, pool.QueryRow(ctx,
		`select (payload->>'count')::int from events where topic = 'schedule.ticks_skipped' and entity_id = $1`,
		scheduleID).Scan(&skipped))
	if skipped != 360 {
		t.Errorf("ticks skipped = %d, want 360", skipped)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestOverlapSkip(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	scheduleID := testdb.NewSchedule(t, ctx, pool, queueID, "* * * * *", "UTC", "skip", "skip", testdb.Epoch)

	live := testdb.Epoch.Add(-time.Minute)
	_, err := pool.Exec(ctx, `insert into jobs
		(project_id, queue_id, schedule_id, type, retry_policy_id, status, scheduled_for, run_at)
		values ($1, $2, $3, 'noop', $4, 'queued', $5, $5)`,
		projectID, queueID, scheduleID, policyID, live)
	testdb.Must(t, err)

	n, err := scheduler.Materialize(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("materialized = %d, want 0", n)
	}

	var tickJobs int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from jobs where schedule_id = $1 and scheduled_for = $2`,
		scheduleID, testdb.Epoch).Scan(&tickJobs))
	if tickJobs != 0 {
		t.Fatalf("jobs for tick = %d, want 0", tickJobs)
	}

	var events int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from events where topic = 'schedule.tick_skipped_overlap' and entity_id = $1`,
		scheduleID).Scan(&events))
	if events != 1 {
		t.Errorf("overlap events = %d, want 1", events)
	}

	var nextRun time.Time
	testdb.Must(t, pool.QueryRow(ctx, `select next_run_at from schedules where id = $1`, scheduleID).Scan(&nextRun))
	if !nextRun.After(testdb.Epoch) {
		t.Errorf("next_run_at = %s, want after %s", nextRun, testdb.Epoch)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestOverlapAllow(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	scheduleID := testdb.NewSchedule(t, ctx, pool, queueID, "* * * * *", "UTC", "allow", "skip", testdb.Epoch)

	live := testdb.Epoch.Add(-time.Minute)
	_, err := pool.Exec(ctx, `insert into jobs
		(project_id, queue_id, schedule_id, type, retry_policy_id, status, scheduled_for, run_at)
		values ($1, $2, $3, 'noop', $4, 'queued', $5, $5)`,
		projectID, queueID, scheduleID, policyID, live)
	testdb.Must(t, err)

	n, err := scheduler.Materialize(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 1 {
		t.Fatalf("materialized = %d, want 1", n)
	}

	var jobs int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from jobs where schedule_id = $1`, scheduleID).Scan(&jobs))
	if jobs != 2 {
		t.Fatalf("jobs for schedule = %d, want 2", jobs)
	}

	testdb.CheckInvariants(t, ctx, pool)
}

func TestPoisonSchedule(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	scheduleID := testdb.NewSchedule(t, ctx, pool, queueID, "not a cron", "UTC", "allow", "skip", testdb.Epoch)

	n, err := scheduler.Materialize(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("materialized = %d, want 0", n)
	}

	var enabled bool
	testdb.Must(t, pool.QueryRow(ctx, `select enabled from schedules where id = $1`, scheduleID).Scan(&enabled))
	if enabled {
		t.Error("schedule enabled = true, want false")
	}

	var errors int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from events where topic = 'schedule.error' and entity_id = $1`, scheduleID).Scan(&errors))
	if errors != 1 {
		t.Errorf("schedule.error events = %d, want 1", errors)
	}

	n, err = scheduler.Materialize(ctx, pool, 100)
	testdb.Must(t, err)
	if n != 0 {
		t.Fatalf("second materialized = %d, want 0", n)
	}

	var jobs int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from jobs where schedule_id = $1`, scheduleID).Scan(&jobs))
	if jobs != 0 {
		t.Errorf("jobs for schedule = %d, want 0", jobs)
	}

	testdb.CheckInvariants(t, ctx, pool)
}
