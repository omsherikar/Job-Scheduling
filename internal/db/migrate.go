package db

import (
	"context"
	"fmt"
	"regexp"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/migrations"
)

const migrateLock = 4265087232

var namePattern = regexp.MustCompile(`^(\d+)_(.+)\.sql$`)

type Migration struct {
	Version string
	Name    string
	SQL     string
}

func Load() ([]Migration, error) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return nil, err
	}

	var out []Migration
	seen := map[string]bool{}

	for _, e := range entries {
		m := namePattern.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		if seen[m[1]] {
			return nil, fmt.Errorf("duplicate migration version %s", m[1])
		}
		seen[m[1]] = true

		body, err := migrations.FS.ReadFile(e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, Migration{Version: m[1], Name: m[2], SQL: string(body)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool, log func(string)) ([]string, error) {
	all, err := Load()
	if err != nil {
		return nil, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "select pg_advisory_lock($1)", migrateLock); err != nil {
		return nil, err
	}
	defer conn.Exec(ctx, "select pg_advisory_unlock($1)", migrateLock)

	_, err = conn.Exec(ctx, `
		create table if not exists schema_migrations (
			version    text primary key,
			name       text not null,
			applied_at timestamptz not null default now()
		)`)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(ctx, "select version from schema_migrations")
	if err != nil {
		return nil, err
	}
	done := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return nil, err
		}
		done[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var applied []string
	for _, m := range all {
		if done[m.Version] {
			continue
		}
		if log != nil {
			log(m.Version + "_" + m.Name)
		}
		if err := apply(ctx, conn.Conn(), m); err != nil {
			return applied, err
		}
		applied = append(applied, m.Version)
	}
	return applied, nil
}

func apply(ctx context.Context, conn *pgx.Conn, m Migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("%s_%s failed: %w", m.Version, m.Name, err)
	}
	_, err = tx.Exec(ctx, "insert into schema_migrations (version, name) values ($1, $2)",
		m.Version, m.Name)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
