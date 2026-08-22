package testdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestClockInjection(t *testing.T) {
	pool := testdb.New(t)
	want := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	testdb.SetNow(t, pool, want)

	for i := 0; i < 20; i++ {
		var got time.Time
		if err := pool.QueryRow(context.Background(), "select fl.now()").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if !got.UTC().Equal(want) {
			t.Fatalf("fl.now() = %v, want %v", got.UTC(), want)
		}
	}

	next := testdb.Advance(t, pool, time.Hour)
	if !next.UTC().Equal(want.Add(time.Hour)) {
		t.Fatalf("advance = %v, want %v", next.UTC(), want.Add(time.Hour))
	}
}
