package worker

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

type finished struct {
	claimed jobs.Claimed
	exec    jobs.Execution
}

func completeLoop(pool *pgxpool.Pool, cfg Config, in <-chan finished) {
	buf := make([]finished, 0, cfg.CompleteBatch)
	tick := time.NewTicker(cfg.CompleteWait)
	defer tick.Stop()

	for {
		select {
		case f, ok := <-in:
			if !ok {
				flush(pool, cfg, buf)
				return
			}
			buf = append(buf, f)
			if len(buf) >= cfg.CompleteBatch {
				buf = flush(pool, cfg, buf)
			}
		case <-tick.C:
			buf = flush(pool, cfg, buf)
		}
	}
}

func flush(pool *pgxpool.Pool, cfg Config, buf []finished) []finished {
	if len(buf) == 0 {
		return buf
	}
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()

	batch := make([]jobs.Done, len(buf))
	for i, f := range buf {
		batch[i] = jobs.Done{JobID: f.claimed.ID, Fence: f.claimed.Fence, Exec: f.exec.ID}
	}

	var done []uuid.UUID
	var err error
	for i := 0; i < reportAttempts; i++ {
		done, err = jobs.CompleteBatch(ctx, pool, batch)
		if err == nil || !transient(err) {
			break
		}
		if !backoff(ctx) {
			break
		}
	}
	if err != nil {
		log.Printf("complete %d jobs: %v", len(batch), err)
		return buf[:0]
	}

	if len(done) < len(buf) {
		ok := make(map[uuid.UUID]bool, len(done))
		for _, id := range done {
			ok[id] = true
		}
		for _, f := range buf {
			if !ok[f.claimed.ID] {
				probe(pool, cfg, f.claimed, f.exec)
			}
		}
	}
	return buf[:0]
}
