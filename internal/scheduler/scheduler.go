package scheduler

import (
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	Promote   time.Duration
	Reap      time.Duration
	Cron      time.Duration
	Breaker   time.Duration
	Age       time.Duration
	Partition time.Duration
	Notify    time.Duration
	Rollup    time.Duration
	Sweep     time.Duration
	Summary   time.Duration

	Batch      int
	AgeAfter   time.Duration
	AgeCeiling int
	Hot        time.Duration
	Cold       time.Duration
	Horizon    time.Duration
	Lookback   time.Duration
	Window     int
	Trip       int
	Cooldown   time.Duration
}

func DefaultConfig() Config {
	return Config{
		Promote:   time.Second,
		Reap:      5 * time.Second,
		Cron:      time.Second,
		Breaker:   5 * time.Second,
		Age:       30 * time.Second,
		Partition: time.Hour,
		Notify:    20 * time.Millisecond,
		Rollup:    time.Minute,
		Sweep:     time.Minute,
		Summary:   5 * time.Minute,

		Batch:      100,
		AgeAfter:   5 * time.Minute,
		AgeCeiling: 3,
		Hot:        7 * 24 * time.Hour,
		Cold:       90 * 24 * time.Hour,
		Horizon:    30 * 24 * time.Hour,
		Lookback:   10 * time.Minute,
		Window:     20,
		Trip:       10,
		Cooldown:   30 * time.Second,
	}
}

func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config) error {
	var wg sync.WaitGroup
	start := func(name string, every time.Duration, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(ctx, name, every, fn)
		}()
	}

	start("promote", cfg.Promote, func(ctx context.Context) error {
		_, err := Promote(ctx, pool, cfg.Batch)
		return err
	})
	start("reap", cfg.Reap, func(ctx context.Context) error {
		_, err := Reap(ctx, pool, cfg.Batch)
		return err
	})
	start("sweep", cfg.Sweep, func(ctx context.Context) error {
		_, err := SweepOrphanExecutions(ctx, pool, cfg.Batch)
		return err
	})
	start("cron", cfg.Cron, func(ctx context.Context) error {
		_, err := Materialize(ctx, pool, cfg.Batch)
		return err
	})
	start("breaker", cfg.Breaker, func(ctx context.Context) error {
		_, err := Breaker(ctx, pool, cfg.Window, cfg.Trip, cfg.Cooldown)
		return err
	})
	start("age", cfg.Age, func(ctx context.Context) error {
		_, err := Age(ctx, pool, cfg.AgeAfter, cfg.AgeCeiling, cfg.Batch)
		return err
	})
	start("partition", cfg.Partition, func(ctx context.Context) error {
		_, err := Maintain(ctx, pool, cfg.Hot, cfg.Cold, cfg.Horizon)
		return err
	})
	start("rollup", cfg.Rollup, func(ctx context.Context) error {
		_, err := Rollup(ctx, pool, cfg.Lookback)
		return err
	})
	start("summary", cfg.Summary, func(ctx context.Context) error {
		_, err := Summarize(ctx, pool, cfg.Batch)
		return err
	})

	var head int64
	start("notify", cfg.Notify, func(ctx context.Context) error {
		n, err := Notify(ctx, pool, head)
		head = n
		return err
	})

	<-ctx.Done()
	wg.Wait()
	return nil
}

func loop(ctx context.Context, name string, every time.Duration, fn func(context.Context) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(every)):
		}
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			log.Printf("%s: %v", name, err)
		}
	}
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}
