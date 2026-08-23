package worker

import (
	"context"
	"log"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

const maxPending = 1000

type logbuf struct {
	mu    sync.Mutex
	lines []jobs.LogLine
}

func (b *logbuf) add(level, message string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.lines) >= maxPending {
		return
	}
	b.lines = append(b.lines, jobs.LogLine{Level: level, Message: message})
}

func (b *logbuf) take() []jobs.LogLine {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.lines
	b.lines = nil
	return out
}

func writeLogs(pool *pgxpool.Pool, exec jobs.Execution, lines []jobs.LogLine) {
	if len(lines) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	if err := jobs.AppendLogs(ctx, pool, exec.ID, lines); err != nil {
		log.Printf("append %d log lines for execution %s: %v", len(lines), exec.ID, err)
	}
}
