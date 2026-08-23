package main

import (
	"context"
	"fmt"
	"os"

	"github.com/shrutu0929/fenceline/internal/db"
)

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is not set")
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := db.Open(ctx, url, 1, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer pool.Close()

	applied, err := db.Migrate(ctx, pool, func(s string) { fmt.Println(s) })
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(applied) == 0 {
		fmt.Println("up to date")
	} else {
		fmt.Printf("applied %d\n", len(applied))
	}
}
