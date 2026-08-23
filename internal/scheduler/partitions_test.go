package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestPartitionDrop(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	old := testdb.Epoch.AddDate(0, 0, -30).UTC()
	day := old.Format("2006-01-02")
	part := "events_" + old.Format("20060102")
	_, err := pool.Exec(ctx, `select fl.ensure_partition('events', $1::date)`, day)
	testdb.Must(t, err)

	dropped, err := scheduler.Maintain(ctx, pool, 7*24*time.Hour, 90*24*time.Hour, 30*24*time.Hour)
	testdb.Must(t, err)
	if dropped < 1 {
		t.Fatalf("dropped = %d, want >= 1", dropped)
	}

	var oldPresent int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		join pg_class p on p.oid = i.inhparent
		where p.relname = 'events' and c.relname = $1`, part).Scan(&oldPresent))
	if oldPresent != 0 {
		t.Errorf("old partition %s present = %d, want 0", part, oldPresent)
	}

	var future string
	testdb.Must(t, pool.QueryRow(ctx,
		`select 'events_' || to_char((fl.now() + interval '7 days')::date, 'YYYYMMDD')`).Scan(&future))
	var futurePresent int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		join pg_class p on p.oid = i.inhparent
		where p.relname = 'events' and c.relname = $1`, future).Scan(&futurePresent))
	if futurePresent != 1 {
		t.Errorf("future partition %s present = %d, want 1", future, futurePresent)
	}
}

func partitionExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, parent, part string) bool {
	t.Helper()
	var n int
	testdb.Must(t, pool.QueryRow(ctx, `select count(*) from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		join pg_class p on p.oid = i.inhparent
		where p.relname = $1 and c.relname = $2`, parent, part).Scan(&n))
	return n == 1
}

func TestColdOutlivesHot(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	old := testdb.Epoch.AddDate(0, 0, -30).UTC()
	day := old.Format("2006-01-02")
	suffix := old.Format("20060102")
	for _, parent := range []string{"events", "jobs_archive"} {
		_, err := pool.Exec(ctx, `select fl.ensure_partition($1, $2::date)`, parent, day)
		testdb.Must(t, err)
	}

	_, err := scheduler.Maintain(ctx, pool, 7*24*time.Hour, 90*24*time.Hour, 30*24*time.Hour)
	testdb.Must(t, err)

	if partitionExists(t, ctx, pool, "events", "events_"+suffix) {
		t.Errorf("events_%s present, want dropped", suffix)
	}
	if !partitionExists(t, ctx, pool, "jobs_archive", "jobs_archive_"+suffix) {
		t.Errorf("jobs_archive_%s dropped, want kept", suffix)
	}
}

func TestPartitionHorizon(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	_, err := scheduler.Maintain(ctx, pool, 7*24*time.Hour, 90*24*time.Hour, 30*24*time.Hour)
	testdb.Must(t, err)

	edge := testdb.Epoch.AddDate(0, 0, 30).UTC().Format("20060102")
	if !partitionExists(t, ctx, pool, "events", "events_"+edge) {
		t.Errorf("events_%s missing, want present", edge)
	}
	if !partitionExists(t, ctx, pool, "job_logs", "job_logs_"+edge) {
		t.Errorf("job_logs_%s missing, want present", edge)
	}
}

func TestColdRetention(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)

	old := testdb.Epoch.AddDate(0, 0, -100).UTC()
	day := old.Format("2006-01-02")
	suffix := old.Format("20060102")
	cold := []string{"jobs_archive", "job_executions_archive", "job_logs_archive", "dead_letter_jobs_archive"}
	for _, parent := range cold {
		_, err := pool.Exec(ctx, `select fl.ensure_partition($1, $2::date)`, parent, day)
		testdb.Must(t, err)
	}

	_, err := scheduler.Maintain(ctx, pool, 7*24*time.Hour, 90*24*time.Hour, 30*24*time.Hour)
	testdb.Must(t, err)

	for _, parent := range cold {
		if partitionExists(t, ctx, pool, parent, parent+"_"+suffix) {
			t.Errorf("%s_%s present, want dropped", parent, suffix)
		}
	}
}
