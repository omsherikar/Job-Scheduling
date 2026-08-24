package scheduler_test

import (
	"context"
	"testing"

	"github.com/shrutu0929/fenceline/internal/scheduler"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestSummarizeSkippedWithoutKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	ctx := context.Background()
	pool := testdb.New(t)

	n, err := scheduler.Summarize(ctx, pool, 10)
	testdb.Must(t, err)
	if n != 0 {
		t.Errorf("summaries written = %d, want 0", n)
	}
}
