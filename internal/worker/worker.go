package worker

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

type Job struct {
	ID      uuid.UUID
	Type    string
	Payload []byte
	Attempt int
	Fence   int64
	Report  func(progress any)
}

type Handler func(ctx context.Context, j Job) error

type Config struct {
	QueueID     uuid.UUID
	WorkerID    uuid.UUID
	Concurrency int
	Lease       time.Duration
	Drain       time.Duration
	Poll        time.Duration

	CompleteBatch int
	CompleteWait  time.Duration
}

func DefaultConfig() Config {
	return Config{
		Concurrency:   4,
		Lease:         30 * time.Second,
		Drain:         30 * time.Second,
		Poll:          time.Second,
		CompleteBatch: 25,
		CompleteWait:  50 * time.Millisecond,
	}
}

func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config, handlers map[string]Handler) error {
	stopCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer stop()

	runCtx, abort := context.WithCancel(context.Background())
	defer abort()

	completions := make(chan finished, cfg.CompleteBatch*2)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		completeLoop(pool, cfg, completions)
	}()

	var wg sync.WaitGroup
	var active atomic.Int64

	for stopCtx.Err() == nil {
		free := cfg.Concurrency - int(active.Load())
		if free > 0 {
			claimed, err := jobs.Claim(stopCtx, pool, jobs.ClaimRequest{
				QueueID:   cfg.QueueID,
				WorkerID:  cfg.WorkerID,
				FreeSlots: free,
				Lease:     cfg.Lease,
			})
			if err != nil && stopCtx.Err() == nil {
				log.Printf("claim queue %s: %v", cfg.QueueID, err)
			}
			for _, c := range claimed {
				active.Add(1)
				wg.Add(1)
				go func(c jobs.Claimed) {
					defer wg.Done()
					defer active.Add(-1)
					runOne(stopCtx, runCtx, pool, cfg, handlers, completions, c)
				}(c)
			}
		}
		wait(stopCtx, jitter(cfg.Poll))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(cfg.Drain):
		abort()
		<-done
	}

	close(completions)
	<-completed
	return ctx.Err()
}
