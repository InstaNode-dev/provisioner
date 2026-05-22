package main

// pool_metrics_extra_test.go — coverage for the pgxpool stats exporter and the
// poolBox / poolProbe seams. The stats path is gated on
// TEST_PROVISIONER_DATABASE_URL (it needs a real *pgxpool.Pool to read
// Stat()); the nil-pool guard and the poolBox round-trip are hermetic.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func rootTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_PROVISIONER_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_PROVISIONER_DATABASE_URL not set — skipping pgxpool stats tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return pool
}

// TestPublishPgxPoolStats_RealPool — publishPgxPoolStats reads pool.Stat() and
// pushes onto the gauges without panicking on a live pool.
func TestPublishPgxPoolStats_RealPool(t *testing.T) {
	pool := rootTestPool(t)
	publishPgxPoolStats(pool, "test_pool")
}

// TestStartPgxPoolStatsExporter_NilPool — the nil-pool guard logs and returns
// immediately rather than dereferencing nil.
func TestStartPgxPoolStatsExporter_NilPool(t *testing.T) {
	done := make(chan struct{})
	go func() {
		startPgxPoolStatsExporter(context.Background(), nil, "nil_pool")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startPgxPoolStatsExporter(nil) did not return immediately")
	}
}

// TestStartPgxPoolStatsExporter_TicksAndStops — with a real pool, the exporter
// publishes once eagerly, ticks, then exits cleanly on ctx cancel. Exercises
// both the initial publish and the ctx.Done() return arm.
func TestStartPgxPoolStatsExporter_TicksAndStops(t *testing.T) {
	pool := rootTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		startPgxPoolStatsExporter(ctx, pool, "exporter_test")
		close(done)
	}()

	// Let the eager publish run, then cancel — the exporter must return.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startPgxPoolStatsExporter did not stop after ctx cancel")
	}
}

// TestPoolBox_RoundTrip — Get returns nil before Set, the stored pool after.
func TestPoolBox_RoundTrip(t *testing.T) {
	box := &poolBox{}
	if box.Get() != nil {
		t.Fatal("fresh poolBox.Get() should be nil")
	}
	pool := rootTestPool(t)
	box.Set(pool)
	if box.Get() != pool {
		t.Fatal("poolBox.Get() did not return the Set pool")
	}
}

// TestPoolProbe_Ping — Ping returns errPoolNotReady when the box is empty and
// pings the real pool once it is set.
func TestPoolProbe_Ping(t *testing.T) {
	box := &poolBox{}
	probe := poolProbe{box: box}

	if err := probe.Ping(context.Background()); !errors.Is(err, errPoolNotReady) {
		t.Fatalf("Ping on empty box = %v, want errPoolNotReady", err)
	}

	box.Set(rootTestPool(t))
	if err := probe.Ping(context.Background()); err != nil {
		t.Fatalf("Ping on a live pool returned %v", err)
	}
}

// TestNewBoundedPgxPoolConfig_ParseError — a malformed DSN must surface the
// pgxpool.ParseConfig error (the early-return arm) rather than a defaulted cfg.
func TestNewBoundedPgxPoolConfig_ParseError(t *testing.T) {
	if _, err := newBoundedPgxPoolConfig("::::not-a-dsn::::"); err == nil {
		t.Fatal("newBoundedPgxPoolConfig should error on a malformed DSN")
	}
}
