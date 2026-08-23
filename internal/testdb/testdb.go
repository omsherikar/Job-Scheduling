package testdb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/db"
)

const (
	template     = "fenceline_test_template"
	templateLock = 4265087233
)

var once sync.Once

func New(t *testing.T) *pgxpool.Pool {
	t.Helper()

	base := baseURL(t)
	var setupErr error
	once.Do(func() { setupErr = ensureTemplate(base) })
	if setupErr != nil {
		t.Fatalf("build template: %v", setupErr)
	}

	ctx := context.Background()
	name := "fl_t_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	admin, err := db.Open(ctx, dbURL(base, "postgres"), 1, 0)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}

	if _, err := admin.Exec(ctx, fmt.Sprintf("create database %s template %s", name, template)); err != nil {
		admin.Close()
		t.Fatalf("clone template: %v", err)
	}

	pool, err := db.Open(ctx, dbURL(base, name), 40, 0)
	if err != nil {
		admin.Close()
		t.Fatalf("connect %s: %v", name, err)
	}

	t.Cleanup(func() {
		pool.Close()
		disconnect(ctx, admin, name)
		admin.Exec(ctx, "drop database if exists "+name)
		admin.Close()
	})

	return pool
}

func ensureTemplate(base string) error {
	ctx := context.Background()

	admin, err := db.Open(ctx, dbURL(base, "postgres"), 1, 0)
	if err != nil {
		return err
	}
	defer admin.Close()

	if _, err := admin.Exec(ctx, "select pg_advisory_lock($1)", templateLock); err != nil {
		return err
	}
	defer admin.Exec(ctx, "select pg_advisory_unlock($1)", templateLock)

	var exists bool
	err = admin.QueryRow(ctx,
		"select exists (select 1 from pg_database where datname = $1)", template).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		if _, err := admin.Exec(ctx, "create database "+template); err != nil {
			return err
		}
	}

	tpl, err := db.Open(ctx, dbURL(base, template), 1, 0)
	if err != nil {
		return err
	}
	_, err = db.Migrate(ctx, tpl, nil)
	tpl.Close()
	if err != nil {
		return err
	}

	disconnect(ctx, admin, template)
	return nil
}

func disconnect(ctx context.Context, admin *pgxpool.Pool, name string) {
	admin.Exec(ctx, `select pg_terminate_backend(pid) from pg_stat_activity
		where datname = $1 and pid <> pg_backend_pid()`, name)
}

func baseURL(t *testing.T) string {
	t.Helper()
	for _, k := range []string{"TEST_DATABASE_URL", "DATABASE_URL"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	t.Skip("TEST_DATABASE_URL is not set")
	return ""
}

func dbURL(base, name string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	u.Path = "/" + name
	return u.String()
}
