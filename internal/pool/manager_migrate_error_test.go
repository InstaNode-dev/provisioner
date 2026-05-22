package pool

// manager_migrate_error_test.go — error arms of migrate / Start / Claim that
// the happy-path integration tests skip. Gated on TEST_PROVISIONER_DATABASE_URL
// (a real pool is needed; we close it to force the failures).

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// closedManager returns a Manager wired to a real-but-closed pool so every DB
// operation fails. The pool is migrated once (against the live DB) before
// closing only when the test needs the table to exist; here we close
// immediately so even migrate fails.
func closedManager(t *testing.T, cfg Config) *Manager {
	t.Helper()
	dsn := testDSN(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	pool.Close()
	return New(pool, make([]byte, 32), cfg, nil, nil, nil, nil)
}

// TestMigrate_Error — migrate over a closed pool must return the wrapped
// create-table error.
func TestMigrate_Error(t *testing.T) {
	m := closedManager(t, Config{})
	if err := m.migrate(context.Background()); err == nil {
		t.Fatal("migrate over a closed pool should error")
	}
}

// TestStart_MigrateError — Start must propagate a migrate failure (closed pool)
// as a wrapped error and not launch the maintenance goroutine.
func TestStart_MigrateError(t *testing.T) {
	m := closedManager(t, Config{PostgresSize: 1})
	if err := m.Start(context.Background()); err == nil {
		t.Fatal("Start should error when migrate fails")
	}
}

// TestClaim_QueryError — Claim over a closed pool hits the scan error arm
// (a connection error, not pgx.ErrNoRows) and returns a wrapped error.
func TestClaim_QueryError(t *testing.T) {
	m := closedManager(t, Config{})
	item, err := m.Claim(context.Background(), "postgres")
	if err == nil {
		t.Fatalf("Claim over a closed pool should error; got item=%+v", item)
	}
	if item != nil {
		t.Fatalf("Claim returned non-nil item on query error: %+v", item)
	}
}
