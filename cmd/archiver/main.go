package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/shrutu0929/fenceline/internal/archiver"
	"github.com/shrutu0929/fenceline/internal/db"
)

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, url, 4)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	log.Print("archiver starting")
	if err := archiver.Run(ctx, pool, archiver.DefaultConfig()); err != nil {
		log.Fatal(err)
	}
	log.Print("archiver stopped")
}
