package testutil

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ordersys/pkg/events"
)

// NewDB applies the real service migrations in an isolated, disposable schema.
func NewDB(t *testing.T, service string) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL regression tests")
	}
	ctx := t.Context()
	admin, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{"test_" + uuid.NewString()}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, file, _, _ := runtime.Caller(0)
	migrations, err := filepath.Glob(filepath.Join(filepath.Dir(file), "../../services", service+"-service/db/migrations/*.up.sql"))
	if err != nil || len(migrations) == 0 {
		t.Fatalf("missing migrations for %s: %v", service, err)
	}
	for _, path := range migrations {
		sql, err := os.ReadFile(path) // #nosec G304 -- Paths come only from the repository migration glob.
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", path, err)
		}
	}
	return pool
}

func Event(t *testing.T, eventType string, payload any) *events.RawEnvelope {
	t.Helper()
	body, err := json.Marshal(events.NewEnvelope("test", eventType, payload))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := events.ParseRaw(body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
