package worker

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

const (
	reportAttempts = 3
	reportBackoff  = 100 * time.Millisecond
	reportTimeout  = 30 * time.Second
)

func runOne(stopCtx, runCtx context.Context, pool *pgxpool.Pool, cfg Config, handlers map[string]Handler, c jobs.Claimed) {
	h, ok := handlers[c.Type]
	if !ok {
		log.Printf("no handler for type %s job %s", c.Type, c.ID)
		return
	}
	if stopCtx.Err() != nil {
		return
	}

	exec, err := startJob(runCtx, pool, cfg, c)
	if err != nil {
		if !errors.Is(err, jobs.ErrFenced) && runCtx.Err() == nil {
			log.Printf("start job %s: %v", c.ID, err)
		}
		return
	}

	jobCtx, cancelJob := context.WithCancel(runCtx)
	defer cancelJob()

	var fenced atomic.Bool
	extDone := make(chan struct{})
	go func() {
		defer close(extDone)
		leaseLoop(jobCtx, pool, cfg, c, &fenced, cancelJob)
	}()

	herr := h(jobCtx, Job{
		ID:      c.ID,
		Type:    c.Type,
		Payload: c.Payload,
		Attempt: exec.Attempt,
		Fence:   c.Fence,
	})

	cancelJob()
	<-extDone

	if fenced.Load() {
		probe(pool, cfg, c, exec)
		return
	}
	if herr == nil {
		report(pool, c, exec, nil)
		return
	}
	if runCtx.Err() != nil {
		return
	}
	report(pool, c, exec, herr)
}

func leaseLoop(ctx context.Context, pool *pgxpool.Pool, cfg Config, c jobs.Claimed, fenced *atomic.Bool, cancelJob context.CancelFunc) {
	t := time.NewTicker(cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ok, err := extend(ctx, pool, cfg, c)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if !ok {
				fenced.Store(true)
				cancelJob()
				return
			}
		}
	}
}

func startJob(ctx context.Context, pool *pgxpool.Pool, cfg Config, c jobs.Claimed) (jobs.Execution, error) {
	var exec jobs.Execution
	var err error
	for i := 0; i < reportAttempts; i++ {
		exec, err = jobs.Start(ctx, pool, c.ID, c.Fence, cfg.WorkerID)
		if err == nil || errors.Is(err, jobs.ErrFenced) || !transient(err) {
			return exec, err
		}
		if !backoff(ctx) {
			return exec, err
		}
	}
	return exec, err
}

func extend(ctx context.Context, pool *pgxpool.Pool, cfg Config, c jobs.Claimed) (bool, error) {
	var ok bool
	var err error
	for i := 0; i < reportAttempts; i++ {
		ok, err = jobs.ExtendLease(ctx, pool, c.ID, c.Fence, cfg.Lease)
		if err == nil || !transient(err) {
			return ok, err
		}
		if !backoff(ctx) {
			return ok, err
		}
	}
	return ok, err
}

func report(pool *pgxpool.Pool, c jobs.Claimed, exec jobs.Execution, herr error) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()

	var err error
	for i := 0; i < reportAttempts; i++ {
		if herr == nil {
			err = jobs.Complete(ctx, pool, c.ID, c.Fence, exec.ID)
		} else {
			err = jobs.Fail(ctx, pool, c.ID, c.Fence, herr)
		}
		if err == nil || errors.Is(err, jobs.ErrFenced) || !transient(err) {
			break
		}
		if !backoff(ctx) {
			break
		}
	}
	if err != nil && !errors.Is(err, jobs.ErrFenced) {
		log.Printf("report job %s: %v", c.ID, err)
	}
}

func probe(pool *pgxpool.Pool, cfg Config, c jobs.Claimed, exec jobs.Execution) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	st, err := jobs.Probe(ctx, pool, c.ID, c.Fence, cfg.WorkerID)
	if err != nil {
		log.Printf("probe job %s: %v", c.ID, err)
		return
	}
	if st == jobs.StatusLive {
		return
	}
	log.Printf("job %s stopped: %s", c.ID, st)

	if err := jobs.CloseExecution(ctx, pool, exec.ID, st); err != nil {
		log.Printf("close execution %s: %v", exec.ID, err)
	}
}

func transient(err error) bool {
	if err == nil {
		return false
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code == "40001" || pg.Code == "40P01"
	}
	return pgconn.SafeToRetry(err)
}

func backoff(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(reportBackoff):
		return true
	}
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func wait(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
