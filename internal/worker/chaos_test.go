package worker_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
	"github.com/shrutu0929/fenceline/internal/worker"
)

const chaosJobs = 400

func TestFaultInjection(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	_, err := pool.Exec(ctx,
		`update retry_policies set max_attempts = 20, base_delay_ms = 1, max_delay_ms = 20 where id = $1`,
		policyID)
	testdb.Must(t, err)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 16)

	for i := 0; i < chaosJobs; i++ {
		testdb.NewJob(t, ctx, pool, projectID, queueID, policyID)
	}

	runCtx, stop := context.WithCancel(ctx)
	var crew sync.WaitGroup

	handler := func(ctx context.Context, j worker.Job) error {
		d := time.Duration(rand.IntN(120)) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
		if rand.IntN(8) == 0 {
			return errors.New("injected")
		}
		return nil
	}

	for w := 0; w < 4; w++ {
		workerID := testdb.NewWorker(t, ctx, pool, projectID)
		cfg := worker.DefaultConfig()
		cfg.QueueID = queueID
		cfg.WorkerID = workerID
		cfg.Concurrency = 4
		cfg.Lease = 150 * time.Millisecond
		cfg.Poll = 20 * time.Millisecond
		cfg.Drain = time.Second
		cfg.Heartbeat = time.Second
		cfg.CompleteWait = 20 * time.Millisecond
		cfg.Listen = false

		crew.Add(1)
		go func() {
			defer crew.Done()
			worker.Run(runCtx, pool, cfg, map[string]worker.Handler{"noop": handler})
		}()
	}

	crew.Add(1)
	go func() {
		defer crew.Done()
		for runCtx.Err() == nil {
			scheduler.Reap(runCtx, pool, 20)
			scheduler.Promote(runCtx, pool, 100)
			time.Sleep(15 * time.Millisecond)
		}
	}()

	crew.Add(1)
	go func() {
		defer crew.Done()
		for runCtx.Err() == nil {
			pool.Exec(runCtx, `select pg_terminate_backend(pid) from pg_stat_activity
				where datname = current_database() and pid <> pg_backend_pid()
				  and state = 'idle in transaction'`)
			time.Sleep(40 * time.Millisecond)
		}
	}()

	crew.Add(1)
	go func() {
		defer crew.Done()
		for runCtx.Err() == nil {
			pool.Exec(runCtx, `update job_leases set expires_at = fl.now() - interval '1 second'
				where job_id in (select job_id from job_leases order by random() limit 2)`)
			time.Sleep(25 * time.Millisecond)
		}
	}()

	settled := waitFor(90*time.Second, func() bool {
		var live int
		if err := pool.QueryRow(ctx,
			`select count(*) from jobs where status not in ('completed', 'dead_letter', 'cancelled')`).
			Scan(&live); err != nil {
			return false
		}
		return live == 0
	})
	stop()
	crew.Wait()

	if !settled {
		t.Fatalf("unsettled jobs = %d, want 0",
			testdb.Count(t, ctx, pool, `select count(*) from jobs where status not in ('completed','dead_letter','cancelled')`))
	}

	dup := testdb.Count(t, ctx, pool, `select count(*) from (
		select job_id from job_executions where outcome = 'success'
		group by job_id, replay_generation having count(*) > 1) d`)
	if dup != 0 {
		t.Errorf("jobs completed more than once = %d, want 0", dup)
	}

	fenced := testdb.Count(t, ctx, pool, `select count(*) from job_fence_violations`)
	if fenced == 0 {
		t.Error("fence violations = 0, want more than 0")
	}

	completed := testdb.Count(t, ctx, pool, `select count(*) from jobs where status = 'completed'`)
	t.Logf("jobs=%d completed=%d fenced writes=%d", chaosJobs, completed, fenced)

	var open int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from job_executions where finished_at is null`).Scan(&open))
	if open != 0 {
		t.Errorf("open executions = %d, want 0", open)
	}

	over := testdb.Count(t, ctx, pool, `select count(*) from queues q where q.in_flight < (
		select count(*) from jobs j where j.queue_id = q.id and j.status in ('claimed', 'running'))`)
	if over != 0 {
		t.Errorf("queues counting fewer in flight than are running = %d, want 0", over)
	}

	repaired, err := jobs.ReconcileInFlight(ctx, pool)
	testdb.Must(t, err)
	t.Logf("queues repaired by reconcile = %d", repaired)
	testdb.CheckInvariants(t, ctx, pool)
}
