package jobs_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func setShards(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID, n int) error {
	t.Helper()
	_, err := pool.Exec(ctx, `update queues set shards = $2 where id = $1`, queueID, n)
	return err
}

func shardCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID) map[int]int {
	t.Helper()
	rows, err := pool.Query(ctx,
		`select shard, in_flight from queue_shards where queue_id = $1 order by shard`, queueID)
	testdb.Must(t, err)
	defer rows.Close()
	out := map[int]int{}
	for rows.Next() {
		var shard, n int
		testdb.Must(t, rows.Scan(&shard, &n))
		out[shard] = n
	}
	testdb.Must(t, rows.Err())
	return out
}

func TestShardedCap(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	testdb.Must(t, setShards(t, ctx, pool, queueID, 4))
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 200)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 12, 3)
	res, errs := drainClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	total, dups := gather(res)
	if dups != 0 {
		t.Errorf("duplicate claims = %d, want 0", dups)
	}
	if total != 10 {
		t.Errorf("claimed = %d, want 10", total)
	}

	counts := shardCounts(t, ctx, pool, queueID)
	if len(counts) != 4 {
		t.Fatalf("shard rows = %d, want 4", len(counts))
	}
	sum := 0
	for shard, n := range counts {
		slot := 10 / 4
		if shard < 10%4 {
			slot++
		}
		if n > slot {
			t.Errorf("shard %d in_flight = %d, want <= %d", shard, n, slot)
		}
		sum += n
	}
	if sum != total {
		t.Errorf("shard in_flight sum = %d, want %d", sum, total)
	}
	testdb.CheckInvariants(t, ctx, pool)
}

func TestShardSpread(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 12)
	testdb.Must(t, setShards(t, ctx, pool, queueID, 4))
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 200)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 12, 1)
	_, errs := drainClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	used := 0
	for _, n := range shardCounts(t, ctx, pool, queueID) {
		if n > 0 {
			used++
		}
	}
	if used < 2 {
		t.Errorf("shards holding work = %d, want at least 2", used)
	}
}

func TestShardShrinkWithWork(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 12)
	testdb.Must(t, setShards(t, ctx, pool, queueID, 4))
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 200)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 12, 1)
	_, errs := drainClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	counts := shardCounts(t, ctx, pool, queueID)
	top := 0
	for shard, n := range counts {
		if n > 0 && shard > top {
			top = shard
		}
	}
	if top == 0 {
		t.Fatalf("no work above shard 0, counts = %v", counts)
	}

	if err := setShards(t, ctx, pool, queueID, top); err == nil {
		t.Fatalf("shrink to %d succeeded while shard %d holds work", top, top)
	}
	if err := setShards(t, ctx, pool, queueID, top+1); err != nil {
		t.Fatalf("shrink to %d: %v", top+1, err)
	}
}

func TestShardReleaseOnComplete(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)

	projectID, policyID := testdb.Base(t, ctx, pool)
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 8)
	testdb.Must(t, setShards(t, ctx, pool, queueID, 4))
	seedJobs(t, ctx, pool, projectID, queueID, policyID, 50)

	reqs := claimReqs(t, ctx, pool, projectID, queueID, 8, 1)
	res, errs := drainClaims(ctx, pool, reqs)
	failOnClaimError(t, errs)

	for i, r := range res {
		for _, c := range r {
			exec, err := jobs.Start(ctx, pool, c.ID, c.Fence, reqs[i].WorkerID)
			testdb.Must(t, err)
			testdb.Must(t, jobs.Complete(ctx, pool, c.ID, c.Fence, exec.ID))
		}
	}

	for shard, n := range shardCounts(t, ctx, pool, queueID) {
		if n != 0 {
			t.Errorf("shard %d in_flight = %d, want 0", shard, n)
		}
	}
	testdb.CheckInvariants(t, ctx, pool)
}
