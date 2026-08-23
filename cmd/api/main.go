package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shrutu0929/fenceline/internal/api"
	"github.com/shrutu0929/fenceline/internal/db"
)

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	addr := os.Getenv("API_ADDR")
	if addr == "" {
		port := os.Getenv("API_PORT")
		if port == "" {
			port = "3001"
		}
		addr = ":" + port
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, url, 20, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	srv := &api.Server{Pool: pool}
	if origins := os.Getenv("API_ALLOWED_ORIGINS"); origins != "" {
		srv.AllowedOrigins = strings.Split(origins, ",")
	}
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("api listening on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Print("api shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutCtx); err != nil {
		log.Print(err)
	}
}
