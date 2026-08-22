package db

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Open(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = filepath.Base(os.Args[0])
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "30s"
	cfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
