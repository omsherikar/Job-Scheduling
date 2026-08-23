package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestNotify(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	projectID, _ := testdb.Base(t, ctx, pool)

	conn, err := pool.Acquire(ctx)
	testdb.Must(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, "listen fl_events")
	testdb.Must(t, err)

	_, err = pool.Exec(ctx, `select fl.emit('a', $1, $1, '{}'::jsonb)`, projectID)
	testdb.Must(t, err)

	head, err := scheduler.Notify(ctx, pool, 0)
	testdb.Must(t, err)
	if head == 0 {
		t.Fatalf("head = %d, want > 0", head)
	}

	wait, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	n, err := conn.Conn().WaitForNotification(wait)
	testdb.Must(t, err)
	if n.Channel != "fl_events" {
		t.Errorf("channel = %q, want fl_events", n.Channel)
	}
	if n.Payload != "" {
		t.Errorf("payload = %q, want empty", n.Payload)
	}
}

func TestNotifyIdle(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	projectID, _ := testdb.Base(t, ctx, pool)

	_, err := pool.Exec(ctx, `select fl.emit('a', $1, $1, '{}'::jsonb)`, projectID)
	testdb.Must(t, err)

	head, err := scheduler.Notify(ctx, pool, 0)
	testdb.Must(t, err)

	conn, err := pool.Acquire(ctx)
	testdb.Must(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, "listen fl_events")
	testdb.Must(t, err)

	again, err := scheduler.Notify(ctx, pool, head)
	testdb.Must(t, err)
	if again != head {
		t.Errorf("head = %d, want %d", again, head)
	}

	wait, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if n, err := conn.Conn().WaitForNotification(wait); err == nil {
		t.Errorf("notification on %q, want none", n.Channel)
	}
}

func TestLowWater(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	projectID, _ := testdb.Base(t, ctx, pool)

	old := testdb.Epoch.AddDate(0, 0, -30).UTC()
	_, err := pool.Exec(ctx, `select fl.ensure_partition('events', $1::date)`, old.Format("2006-01-02"))
	testdb.Must(t, err)

	testdb.SetNow(t, pool, old)
	_, err = pool.Exec(ctx, `select fl.emit('old', $1, $1, '{}'::jsonb)`, projectID)
	testdb.Must(t, err)

	testdb.SetNow(t, pool, testdb.Epoch)
	_, err = pool.Exec(ctx, `select fl.emit('new', $1, $1, '{}'::jsonb)`, projectID)
	testdb.Must(t, err)

	var kept int64
	testdb.Must(t, pool.QueryRow(ctx, `select id from events where topic = 'new'`).Scan(&kept))

	_, err = scheduler.Maintain(ctx, pool, 7*24*time.Hour, 90*24*time.Hour, 30*24*time.Hour)
	testdb.Must(t, err)

	var low int64
	testdb.Must(t, pool.QueryRow(ctx, `select low_water_id from events_retention`).Scan(&low))
	if low != kept {
		t.Errorf("low water = %d, want %d", low, kept)
	}
}
