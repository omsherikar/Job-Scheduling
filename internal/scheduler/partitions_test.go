package scheduler_test

import (
	"context"
	"testing"
	"time"

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

	dropped, err := scheduler.Maintain(ctx, pool, 7*24*time.Hour)
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
