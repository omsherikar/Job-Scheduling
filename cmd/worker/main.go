package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/shrutu0929/fenceline/internal/db"
	"github.com/shrutu0929/fenceline/internal/worker"
)

var handlers = map[string]worker.Handler{
	"noop": func(ctx context.Context, j worker.Job) error { return nil },

	"sleep": func(ctx context.Context, j worker.Job) error {
		var p struct {
			Ms int `json:"ms"`
		}
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return errors.New("bad sleep payload")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(p.Ms) * time.Millisecond):
			return nil
		}
	},

	"fail": func(ctx context.Context, j worker.Job) error {
		return errors.New("deliberate failure")
	},
}

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	queueID, err := uuid.Parse(os.Getenv("WORKER_QUEUE"))
	if err != nil {
		log.Fatal("WORKER_QUEUE must be a queue id")
	}
	concurrency := 4
	if v := os.Getenv("WORKER_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			log.Fatal("WORKER_CONCURRENCY must be a positive number")
		}
		concurrency = n
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, url, int32(concurrency+4))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	workerID, err := worker.Register(ctx, pool, queueID, concurrency)
	if err != nil {
		log.Fatal(err)
	}

	cfg := worker.DefaultConfig()
	cfg.QueueID = queueID
	cfg.WorkerID = workerID
	cfg.Concurrency = concurrency

	log.Printf("worker %s on queue %s, concurrency %d", workerID, queueID, concurrency)
	if err := worker.Run(ctx, pool, cfg, handlers); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
	log.Print("worker stopped")
}
