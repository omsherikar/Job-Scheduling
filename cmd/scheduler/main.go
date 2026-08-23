package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shrutu0929/fenceline/internal/db"
	"github.com/shrutu0929/fenceline/internal/scheduler"
)

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, url, 10, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	log.Print("scheduler starting")
	if err := scheduler.Run(ctx, pool, scheduler.DefaultConfig()); err != nil {
		log.Fatal(err)
	}
	log.Print("scheduler stopped")
}
