package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestOutboxOrder(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	projectID, _ := testdb.Base(t, ctx, pool)

	first, err := pool.Begin(ctx)
	testdb.Must(t, err)
	defer first.Rollback(ctx)
	_, err = first.Exec(ctx, `select fl.emit('a', $1, $1, '{}'::jsonb)`, projectID)
	testdb.Must(t, err)

	second, err := pool.Begin(ctx)
	testdb.Must(t, err)
	defer second.Rollback(ctx)

	done := make(chan error, 1)
	go func() {
		_, err := second.Exec(ctx, `select fl.emit('b', $1, $1, '{}'::jsonb)`, projectID)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("second emit err = %v, want blocked", err)
	case <-time.After(200 * time.Millisecond):
	}

	testdb.Must(t, first.Commit(ctx))
	testdb.Must(t, <-done)
	testdb.Must(t, second.Commit(ctx))

	var a, b int64
	testdb.Must(t, pool.QueryRow(ctx, `select id from events where topic = 'a'`).Scan(&a))
	testdb.Must(t, pool.QueryRow(ctx, `select id from events where topic = 'b'`).Scan(&b))
	if a >= b {
		t.Errorf("ids = %d, %d, want the committed-first event to be lower", a, b)
	}
}

func TestOutboxNoGap(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	projectID, _ := testdb.Base(t, ctx, pool)

	const writers = 24
	start := make(chan struct{})
	done := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() {
			<-start
			tx, err := pool.Begin(ctx)
			if err != nil {
				done <- err
				return
			}
			if _, err := tx.Exec(ctx, `select fl.emit('x', $1, $1, '{}'::jsonb)`, projectID); err != nil {
				tx.Rollback(ctx)
				done <- err
				return
			}
			done <- tx.Commit(ctx)
		}()
	}
	close(start)
	for i := 0; i < writers; i++ {
		testdb.Must(t, <-done)
	}

	seen := 0
	cursor := int64(0)
	for {
		rows, err := pool.Query(ctx,
			`select id from events where project_id = $1 and id > $2 order by id`, projectID, cursor)
		testdb.Must(t, err)
		batch := 0
		for rows.Next() {
			var id int64
			testdb.Must(t, rows.Scan(&id))
			cursor = id
			batch++
		}
		rows.Close()
		testdb.Must(t, rows.Err())
		seen += batch
		if batch == 0 {
			break
		}
	}
	if seen != writers {
		t.Errorf("tailed events = %d, want %d", seen, writers)
	}

	var gaps int
	testdb.Must(t, pool.QueryRow(ctx,
		`select count(*) from (select id - lag(id) over (order by id) as d
		   from events where project_id = $1) g where d <> 1`, projectID).Scan(&gaps))
	if gaps != 0 {
		t.Errorf("id gaps = %d, want 0", gaps)
	}
}

func TestOutboxProjectsIndependent(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	one, _ := testdb.Base(t, ctx, pool)
	two, _ := testdb.Base(t, ctx, pool)

	held, err := pool.Begin(ctx)
	testdb.Must(t, err)
	defer held.Rollback(ctx)
	_, err = held.Exec(ctx, `select fl.emit('a', $1, $1, '{}'::jsonb)`, one)
	testdb.Must(t, err)

	other, err := pool.Begin(ctx)
	testdb.Must(t, err)
	defer other.Rollback(ctx)

	done := make(chan error, 1)
	go func() {
		_, err := other.Exec(ctx, `select fl.emit('b', $1, $1, '{}'::jsonb)`, two)
		done <- err
	}()

	select {
	case err := <-done:
		testdb.Must(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("second-project emit blocked, want unblocked")
	}
	testdb.Must(t, other.Commit(ctx))
	testdb.Must(t, held.Commit(ctx))
}
