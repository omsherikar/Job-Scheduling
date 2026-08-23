package jobs_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func seedJobs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, queueID, policyID uuid.UUID, n int) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	testdb.Must(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "set local statement_timeout = '180s'")
	testdb.Must(t, err)
	_, err = tx.Exec(ctx, `insert into jobs (project_id, queue_id, type, retry_policy_id, status, run_at)
		select $1, $2, 'noop', $3, 'queued', fl.now() from generate_series(1, $4)`,
		projectID, queueID, policyID, n)
	testdb.Must(t, err)
	testdb.Must(t, tx.Commit(ctx))
}

func claimReqs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID, queueID uuid.UUID, n, freeSlots int) []jobs.ClaimRequest {
	t.Helper()
	reqs := make([]jobs.ClaimRequest, n)
	for i := range reqs {
		reqs[i] = jobs.ClaimRequest{
			QueueID:   queueID,
			WorkerID:  testdb.NewWorker(t, ctx, pool, projectID),
			FreeSlots: freeSlots,
			Lease:     30 * time.Second,
		}
	}
	return reqs
}

func runClaims(ctx context.Context, pool *pgxpool.Pool, reqs []jobs.ClaimRequest) ([][]jobs.Claimed, []error) {
	res := make([][]jobs.Claimed, len(reqs))
	errs := make([]error, len(reqs))
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(reqs))
	for i := range reqs {
		go func(i int) {
			defer wg.Done()
			<-start
			res[i], errs[i] = jobs.Claim(ctx, pool, reqs[i])
		}(i)
	}
	close(start)
	wg.Wait()
	return res, errs
}

func drainClaims(ctx context.Context, pool *pgxpool.Pool, reqs []jobs.ClaimRequest) ([][]jobs.Claimed, []error) {
	res := make([][]jobs.Claimed, len(reqs))
	errs := make([]error, len(reqs))
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(reqs))
	for i := range reqs {
		go func(i int) {
			defer wg.Done()
			<-start
			for {
				c, err := jobs.Claim(ctx, pool, reqs[i])
				if err != nil {
					errs[i] = err
					return
				}
				if len(c) == 0 {
					return
				}
				res[i] = append(res[i], c...)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	return res, errs
}

func failOnClaimError(t *testing.T, errs []error) {
	t.Helper()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
	}
}

func gather(res [][]jobs.Claimed) (int, int) {
	seen := map[uuid.UUID]bool{}
	total, dups := 0, 0
	for _, r := range res {
		for _, c := range r {
			total++
			if seen[c.ID] {
				dups++
			}
			seen[c.ID] = true
		}
	}
	return total, dups
}

func claimedCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID) int {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from jobs where queue_id = $1 and status in ('claimed', 'running')`, queueID).Scan(&n))
	return n
}

func TestClaimExactlyOnce(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 200)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 20, 10)
	res, errs := drainClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	total, dups := gather(res)
	if dups != 0 {
		t.Errorf("duplicate claims = %d, want 0", dups)
	}
	if total != 200 {
		t.Errorf("claimed = %d, want 200", total)
	}
	testdb.CheckInvariants(t, ctx, pool)
}

func TestClaimCapHolds(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	const cap = 4

	for _, n := range []int{2, 4, 8, 16, 32} {
		t.Run(fmt.Sprintf("workers=%d", n), func(t *testing.T) {
			queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, cap)
			seedJobs(t, ctx, pool, projectID, queueID, policyID, 300)

			reqs := claimReqs(t, ctx, pool, projectID, queueID, n, 25)
			res, errs := runClaims(ctx, pool, reqs)
			failOnClaimError(t, errs)

			if got := claimedCount(t, ctx, pool, queueID); got != cap {
				t.Errorf("claimed+running = %d, want %d", got, cap)
			}
			if _, dups := gather(res); dups != 0 {
				t.Errorf("duplicate claims = %d, want 0", dups)
			}
			testdb.CheckInvariants(t, ctx, pool)
		})
	}
}

func TestPausedQueue(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 50)
	_, err := pool.Exec(ctx, `update queues set paused = true where id = $1`, queueID)
	testdb.Must(t, err)

	req := jobs.ClaimRequest{
		QueueID:   queueID,
		WorkerID:  testdb.NewWorker(t, ctx, pool, projectID),
		FreeSlots: 25,
		Lease:     30 * time.Second,
	}
	claimed, err := jobs.Claim(ctx, pool, req)
	testdb.Must(t, err)

	if len(claimed) != 0 {
		t.Errorf("claimed = %d, want 0", len(claimed))
	}
	if got := claimedCount(t, ctx, pool, queueID); got != 0 {
		t.Errorf("claimed+running = %d, want 0", got)
	}
	var flight int
	testdb.Must(t, pool.QueryRow(ctx, `select in_flight from queues where id = $1`, queueID).Scan(&flight))
	if flight != 0 {
		t.Errorf("queues.in_flight = %d, want 0", flight)
	}
}

func TestTokenBucket(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 50)
	_, err := pool.Exec(ctx, `update queues
		set rl_limit_per_sec = 1, rl_burst = 10, rl_tokens = 10, rl_refilled_at = fl.now()
		where id = $1`, queueID)
	testdb.Must(t, err)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 8, 5)
	res, errs := runClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	total, dups := gather(res)
	if dups != 0 {
		t.Errorf("duplicate claims = %d, want 0", dups)
	}
	if total != 10 {
		t.Errorf("claimed = %d, want 10", total)
	}
}

func TestOpenBreaker(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 50)
	_, err := pool.Exec(ctx, `update queues
		set breaker_state = 'open', breaker_open_until = fl.now() + interval '1 hour', breaker_probe_budget = 0
		where id = $1`, queueID)
	testdb.Must(t, err)

	req := jobs.ClaimRequest{
		QueueID:   queueID,
		WorkerID:  testdb.NewWorker(t, ctx, pool, projectID),
		FreeSlots: 25,
		Lease:     30 * time.Second,
	}
	claimed, err := jobs.Claim(ctx, pool, req)
	testdb.Must(t, err)

	if len(claimed) != 0 {
		t.Errorf("claimed = %d, want 0", len(claimed))
	}
	if got := claimedCount(t, ctx, pool, queueID); got != 0 {
		t.Errorf("claimed+running = %d, want 0", got)
	}
}

func TestHalfOpenProbe(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 50)
	_, err := pool.Exec(ctx, `update queues
		set breaker_state = 'half_open', breaker_probe_budget = 1 where id = $1`, queueID)
	testdb.Must(t, err)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 8, 5)
	res, errs := runClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	total, _ := gather(res)
	if total != 1 {
		t.Errorf("probes admitted = %d, want 1", total)
	}
	if got := claimedCount(t, ctx, pool, queueID); got != 1 {
		t.Errorf("claimed+running = %d, want 1", got)
	}
}

func TestBreakerCooldownProbe(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 50)
	_, err := pool.Exec(ctx, `update queues
		set breaker_state = 'open', breaker_open_until = fl.now() - interval '1 hour', breaker_probe_budget = 0
		where id = $1`, queueID)
	testdb.Must(t, err)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 8, 5)
	res, errs := runClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	total, _ := gather(res)
	if total != 1 {
		t.Errorf("probes admitted = %d, want 1", total)
	}

	var state string
	testdb.Must(t, pool.QueryRow(ctx, `select breaker_state from queues where id = $1`, queueID).Scan(&state))
	if state != "half_open" {
		t.Errorf("breaker_state = %q, want half_open", state)
	}
}

func TestClaimPlan(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 200000)
	_, err := pool.Exec(ctx, `analyze jobs`)
	testdb.Must(t, err)

	sql := fmt.Sprintf(`explain (format text)
		select j.id from jobs j
		where j.queue_id = '%s' and j.status = 'queued' and j.run_at <= fl.now()
		order by j.priority desc, j.run_at asc, j.id asc
		for update skip locked
		limit 10`, queueID)

	rows, err := pool.Query(ctx, sql)
	testdb.Must(t, err)
	var plan strings.Builder
	for rows.Next() {
		var line string
		testdb.Must(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	testdb.Must(t, rows.Err())

	p := plan.String()
	if !strings.Contains(p, "idx_jobs_claimable") {
		t.Errorf("plan does not use idx_jobs_claimable:\n%s", p)
	}
	if strings.Contains(p, "Seq Scan on jobs") {
		t.Errorf("plan has a seq scan on jobs:\n%s", p)
	}
}

func TestClaimPlanGeneric(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 1000)
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 200000)
	_, err := pool.Exec(ctx, `analyze jobs`)
	testdb.Must(t, err)

	conn, err := pool.Acquire(ctx)
	testdb.Must(t, err)
	defer conn.Release()

	for _, stmt := range []string{
		`set plan_cache_mode = force_generic_plan`,
		`prepare cand(uuid, int) as
		   select j.id from jobs j
		    where j.queue_id = $1 and j.status = 'queued' and j.run_at <= fl.now()
		    order by j.priority desc, j.run_at asc, j.id asc
		    for update skip locked limit $2`,
	} {
		_, err = conn.Exec(ctx, stmt)
		testdb.Must(t, err)
	}

	rows, err := conn.Query(ctx, fmt.Sprintf(`explain execute cand('%s', 10)`, queueID))
	testdb.Must(t, err)
	var plan strings.Builder
	for rows.Next() {
		var line string
		testdb.Must(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	testdb.Must(t, rows.Err())

	p := plan.String()
	if !strings.Contains(p, "idx_jobs_claimable") {
		t.Errorf("generic plan does not use idx_jobs_claimable:\n%s", p)
	}
	if strings.Contains(p, "Seq Scan on jobs") {
		t.Errorf("generic plan has a seq scan on jobs:\n%s", p)
	}
}
