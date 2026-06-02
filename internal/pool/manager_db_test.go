package pool

// manager_db_test.go — integration coverage for the DB-touching seams of the
// hot-pool Manager: migrate, Start, Claim, Stats, fillPool, provisionOneItem,
// and the full New → Start → refill → Claim → Shutdown lifecycle.
//
// These exercise the real Postgres path (the concurrency/shutdown unit tests
// deliberately avoid m.db). They require a throwaway Postgres reachable via
// TEST_PROVISIONER_DATABASE_URL; when it is unset the whole file skips so the
// hermetic unit suite still runs on a DB-less machine. Each test runs in its
// own freshly-truncated pool_items table so ordering / count assertions are
// deterministic.

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"instant.dev/common/crypto"
)

// testDSN returns the throwaway Postgres DSN or skips the test. Centralised so
// every DB test shares the same skip message.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_PROVISIONER_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_PROVISIONER_DATABASE_URL not set — skipping pool DB integration tests")
	}
	return dsn
}

// newDBManager builds a Manager wired to a real pgxpool plus sleeping mock
// backends, runs migrate, and truncates pool_items so each test starts clean.
// It registers cleanup that closes the pool. targets is supplied per-test.
func newDBManager(t *testing.T, cfg Config) (*Manager, *pgxpool.Pool, *concurrencyTracker) {
	t.Helper()
	dsn := testDSN(t)

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

	tr := &concurrencyTracker{}
	aesKey := make([]byte, 32)
	m := New(pool, aesKey, cfg,
		&mockPostgresBackend{tr: tr},
		&mockRedisBackend{tr: tr},
		&mockMongoBackend{tr: tr},
		&mockQueueBackend{tr: tr},
	)

	// migrate creates the table; then truncate so a previous test's rows do not
	// leak into count/order assertions.
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE pool_items`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return m, pool, tr
}

func readyCount(t *testing.T, pool *pgxpool.Pool, rt string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pool_items WHERE resource_type=$1 AND status='ready'`, rt,
	).Scan(&n); err != nil {
		t.Fatalf("count ready: %v", err)
	}
	return n
}

// TestMigrate_Idempotent — migrate must be safe to run twice (it ships
// CREATE TABLE IF NOT EXISTS + ALTER ... ADD COLUMN IF NOT EXISTS).
func TestMigrate_Idempotent(t *testing.T) {
	m, _, _ := newDBManager(t, Config{})
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

// TestProvisionOneItem_InsertsRow — the slow-backend + INSERT seam. After one
// call the pool_items table holds exactly one ready row for the type, with the
// connection_url stored encrypted (decryptable with the manager's key).
func TestProvisionOneItem_InsertsRow(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{})

	if err := m.provisionOneItem(context.Background(), "postgres"); err != nil {
		t.Fatalf("provisionOneItem: %v", err)
	}
	if got := readyCount(t, pool, "postgres"); got != 1 {
		t.Fatalf("ready postgres count = %d, want 1", got)
	}

	// The stored URL must be ciphertext (decrypt round-trips with the key).
	var encURL, poolToken string
	if err := pool.QueryRow(context.Background(),
		`SELECT connection_url, pool_token FROM pool_items WHERE resource_type='postgres' LIMIT 1`,
	).Scan(&encURL, &poolToken); err != nil {
		t.Fatalf("select row: %v", err)
	}
	plain, err := crypto.Decrypt(m.aesKey, encURL)
	if err != nil {
		t.Fatalf("decrypt stored url: %v", err)
	}
	if plain == encURL {
		t.Fatal("connection_url stored in plaintext — must be encrypted at rest")
	}
	if poolToken == "" {
		t.Fatal("pool_token not persisted")
	}
}

// TestProvisionOneItem_UnknownType — an unknown resource type must surface the
// backend error and insert nothing.
func TestProvisionOneItem_UnknownType(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{})
	if err := m.provisionOneItem(context.Background(), "elasticsearch"); err == nil {
		t.Fatal("expected error for unknown resource type")
	}
	if got := readyCount(t, pool, "elasticsearch"); got != 0 {
		t.Fatalf("unknown type left %d rows; want 0", got)
	}
}

// TestFillPool_TopsUpToTarget — fillPool must provision exactly enough items to
// reach the configured target, and be a no-op once at/above target.
func TestFillPool_TopsUpToTarget(t *testing.T) {
	const target = 4
	m, pool, _ := newDBManager(t, Config{RedisSize: target})

	m.fillPool(context.Background(), "redis")
	if got := readyCount(t, pool, "redis"); got != target {
		t.Fatalf("after first fill ready=%d, want %d", got, target)
	}

	// Second fill is a no-op — already at target.
	m.fillPool(context.Background(), "redis")
	if got := readyCount(t, pool, "redis"); got != target {
		t.Fatalf("after second fill ready=%d, want %d (should be no-op)", got, target)
	}
}

// TestFillPool_ZeroTarget — a resource type with target 0 must never provision.
func TestFillPool_ZeroTarget(t *testing.T) {
	m, pool, tr := newDBManager(t, Config{}) // all sizes zero
	m.fillPool(context.Background(), "postgres")
	if got := readyCount(t, pool, "postgres"); got != 0 {
		t.Fatalf("zero-target fill provisioned %d items; want 0", got)
	}
	if tr.Peak() != 0 {
		t.Fatalf("zero-target fill called backend %d times; want 0", tr.Peak())
	}
}

// TestClaim_ReturnsItem_AndDecrementsPool — Claim hands back a decrypted item
// and marks the row assigned (so the ready count drops by one). FIFO order:
// the oldest ready row is claimed first.
func TestClaim_ReturnsItem_AndDecrementsPool(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{MongoSize: 3})
	m.fillPool(context.Background(), "mongodb")
	before := readyCount(t, pool, "mongodb")
	if before != 3 {
		t.Fatalf("setup: ready=%d, want 3", before)
	}

	item, err := m.Claim(context.Background(), "mongodb")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if item == nil {
		t.Fatal("Claim returned nil on a non-empty pool")
	}
	if item.ResourceType != "mongodb" {
		t.Errorf("item.ResourceType = %q, want mongodb", item.ResourceType)
	}
	if item.ConnectionURL == "" {
		t.Error("item.ConnectionURL empty — decrypt should have populated it")
	}
	if item.PoolToken == "" {
		t.Error("item.PoolToken empty")
	}
	if got := readyCount(t, pool, "mongodb"); got != before-1 {
		t.Fatalf("ready after claim = %d, want %d", got, before-1)
	}
}

// TestClaim_EmptyPool_ReturnsNilNil — Claim on an empty pool returns (nil, nil)
// so the caller falls back to live provisioning.
func TestClaim_EmptyPool_ReturnsNilNil(t *testing.T) {
	m, _, _ := newDBManager(t, Config{})
	item, err := m.Claim(context.Background(), "postgres")
	if err != nil {
		t.Fatalf("Claim on empty pool returned err: %v", err)
	}
	if item != nil {
		t.Fatalf("Claim on empty pool returned %+v, want nil", item)
	}
}

// TestStats_CountsReadyPerType — Stats returns the ready count grouped by type,
// and omits types with no ready rows.
func TestStats_CountsReadyPerType(t *testing.T) {
	m, _, _ := newDBManager(t, Config{PostgresSize: 2, RedisSize: 3})
	m.fillPool(context.Background(), "postgres")
	m.fillPool(context.Background(), "redis")

	stats, err := m.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats["postgres"] != 2 {
		t.Errorf("stats[postgres] = %d, want 2", stats["postgres"])
	}
	if stats["redis"] != 3 {
		t.Errorf("stats[redis] = %d, want 3", stats["redis"])
	}
	if _, present := stats["mongodb"]; present {
		t.Errorf("stats should omit mongodb (no ready rows), got %d", stats["mongodb"])
	}
}

// TestStartShutdown_Lifecycle — the full New → Start → initial refill → Claim →
// Shutdown path. Start triggers an async refill for every configured type; we
// poll until the postgres pool is non-empty, claim from it, then Shutdown must
// return promptly (its runCtx cancel + done close + wg.Wait).
func TestStartShutdown_Lifecycle(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{PostgresSize: 2})

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for the async initial refill to land at least one ready item.
	deadline := time.Now().Add(5 * time.Second)
	for readyCount(t, pool, "postgres") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("initial refill did not produce a ready item within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}

	item, err := m.Claim(context.Background(), "postgres")
	if err != nil {
		t.Fatalf("Claim after Start: %v", err)
	}
	if item == nil {
		t.Fatal("Claim returned nil after a populated initial refill")
	}

	done := make(chan struct{})
	go func() { m.Shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return within 5s")
	}
}

// TestManager_Discard_MarksFailed covers bug bash #3: Discard flips a claimed
// (assigned) item to 'failed' so it is never handed out again and a sweeper
// can reclaim it. DB-gated (skips without TEST_PROVISIONER_DATABASE_URL).
func TestManager_Discard_MarksFailed(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{})
	ctx := context.Background()

	var id string
	if err := pool.QueryRow(ctx, `
		INSERT INTO pool_items (resource_type, connection_url, status)
		VALUES ('postgres', 'enc-url', 'assigned')
		RETURNING id
	`).Scan(&id); err != nil {
		t.Fatalf("seed assigned pool_item: %v", err)
	}

	if err := m.Discard(ctx, &Item{ID: id, ResourceType: "postgres"}); err != nil {
		t.Fatalf("Discard: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM pool_items WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("select status: %v", err)
	}
	if status != "failed" {
		t.Errorf("status = %q; want 'failed' after Discard", status)
	}

	// A nil item is a no-op (defensive guard).
	if err := m.Discard(ctx, nil); err != nil {
		t.Errorf("Discard(nil) should be a no-op, got %v", err)
	}
}
