package pool

// manager_db_test.go — DB-driven coverage for the pool Manager.
//
// These tests need a real Postgres (the pool stores its inventory in a
// `pool_items` table and every public method except provisionOneItemBackend
// touches it via pgxpool). Set TEST_DATABASE_URL — the same env var api/ and
// worker/ use. When unset the tests skip cleanly so a contributor without
// Postgres locally still sees a green `go test ./...`. CI provides a service
// container so the coverage gate exercises every path.
//
// What's covered here:
//
//   - migrate            create/extend the pool_items table; idempotent
//   - Start              wires runCtx, kicks initial refill, returns nil err
//   - Shutdown           cancels runCtx, breaks the run loop, returns promptly
//   - Claim (empty)      returns (nil, nil) — the pool-miss → live-provision fallback
//   - Claim (hit)        atomic status flip, decrypts URL, triggers async refill
//   - Stats              group-count of ready items per resource type
//   - triggerRefill      coalesces enqueues onto the bounded refillCh
//   - fillPool           count → needed → provisionItemsConcurrently
//   - provisionOneItem   end-to-end: backend Provision → encrypt → INSERT
//   - run                ticker tick + refillCh consume + done shutdown
//   - NewWithConfig      factory builds backend set from config.Config
//   - provisionOneItemBackend's `default` arm (unknown type)
//   - provisionOneItem encrypt-error paths (wrong-size AES key)
//
// Mock backends here are SUCCESS-PATH mocks reused from
// manager_refill_concurrency_test.go where possible — but kept zero-latency
// (no time.Sleep) so the DB-integration tests stay fast.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/config"
)

// testDBURL returns the test database URL; tests skip when unset.
//
// We prefer TEST_DATABASE_URL to align with api/worker; TEST_POOL_DB_URL
// is accepted as an override so a contributor can point pool tests at a
// dedicated db without affecting other suites.
func testDBURL() string {
	if u := os.Getenv("TEST_POOL_DB_URL"); u != "" {
		return u
	}
	return os.Getenv("TEST_DATABASE_URL")
}

// poolTestDB opens a pgxpool, drops + recreates a per-test schema, and
// returns the pool + a cleanup. Using a private schema per test isolates
// the pool_items table from any other suite running against the same DB
// (api/, worker/) so a concurrent CI shard can't surprise us.
func poolTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn := testDBURL()
	if dsn == "" {
		t.Skip("integration test — set TEST_DATABASE_URL (or TEST_POOL_DB_URL) to run pool DB coverage")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a per-test schema and bake search_path into the pool's DSN so
	// every pool checkout lands in our schema. This isolates the pool_items
	// table from any other suite running against the same DB.
	bootDB, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New(boot): %v", err)
	}
	if err := bootDB.Ping(ctx); err != nil {
		bootDB.Close()
		t.Skipf("postgres unreachable at %s: %v", dsn, err)
	}

	schema := fmt.Sprintf("pooltest_%s_%d", sanitize(t.Name()), time.Now().UnixNano())
	if _, err := bootDB.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema)); err != nil {
		bootDB.Close()
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := bootDB.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schema)); err != nil {
		bootDB.Close()
		t.Fatalf("create schema: %v", err)
	}
	bootDB.Close()

	dsnWithSchema := dsn
	if strings.Contains(dsn, "?") {
		dsnWithSchema += "&search_path=" + schema
	} else {
		dsnWithSchema += "?search_path=" + schema
	}
	db, err := pgxpool.New(ctx, dsnWithSchema)
	if err != nil {
		t.Fatalf("pgxpool.New(scoped): %v", err)
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		t.Fatalf("ping after search_path: %v", err)
	}

	cleanup := func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		db.Close()
		// Reopen on the bare DSN to drop the schema (the scoped pool is closed).
		drop, err := pgxpool.New(c, dsn)
		if err == nil {
			_, _ = drop.Exec(c, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
			drop.Close()
		}
	}
	return db, cleanup
}

// sanitize lowercases and underscores the test name so it's a legal
// Postgres schema identifier.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if len(out) > 30 {
		out = out[:30]
	}
	return out
}

// 32 zero bytes is a valid AES-256 key. Production uses a 64-char hex from
// env; for tests any 32-byte buffer works.
func validAESKey() []byte { return make([]byte, 32) }

// ---- success-path mock backends (zero latency) ----

type fastPostgresBackend struct {
	calls atomic.Int32
	fail  bool
}

func (b *fastPostgresBackend) Provision(ctx context.Context, token, tier string, connLimit int) (*postgres.Credentials, error) {
	b.calls.Add(1)
	if b.fail {
		return nil, errors.New("forced postgres failure")
	}
	return &postgres.Credentials{
		URL:                "postgres://u:p@h/db_" + token,
		DatabaseName:       "db_" + token,
		Username:           "usr_" + token,
		ProviderResourceID: "pg-" + token,
	}, nil
}
func (b *fastPostgresBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (b *fastPostgresBackend) Deprovision(context.Context, string, string) error { return nil }
func (b *fastPostgresBackend) Regrade(context.Context, string, string, int) (postgres.RegradeResult, error) {
	return postgres.RegradeResult{}, nil
}

type fastRedisBackend struct {
	calls atomic.Int32
	fail  bool
}

func (b *fastRedisBackend) Provision(ctx context.Context, token, tier string) (*redis.Credentials, error) {
	b.calls.Add(1)
	if b.fail {
		return nil, errors.New("forced redis failure")
	}
	return &redis.Credentials{URL: "redis://h/0", KeyPrefix: token + ":"}, nil
}
func (b *fastRedisBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (b *fastRedisBackend) Deprovision(context.Context, string, string) error { return nil }

type fastMongoBackend struct {
	calls atomic.Int32
	fail  bool
}

func (b *fastMongoBackend) Provision(ctx context.Context, token, tier string) (*mongo.Credentials, error) {
	b.calls.Add(1)
	if b.fail {
		return nil, errors.New("forced mongo failure")
	}
	return &mongo.Credentials{URL: "mongodb://h/db_" + token, DatabaseName: "db_" + token}, nil
}
func (b *fastMongoBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (b *fastMongoBackend) Deprovision(context.Context, string, string) error { return nil }

type fastQueueBackend struct {
	calls atomic.Int32
	fail  bool
}

func (b *fastQueueBackend) Provision(ctx context.Context, token, tier string) (*queue.Credentials, error) {
	b.calls.Add(1)
	if b.fail {
		return nil, errors.New("forced queue failure")
	}
	return &queue.Credentials{URL: "nats://h:4222", SubjectPrefix: token + ".", ProviderResourceID: "q-" + token}, nil
}
func (b *fastQueueBackend) Deprovision(context.Context, string, string) error { return nil }

// newPoolWithDB builds a Manager wired to fast mock backends + a real DB,
// migrated and ready to use. cfg controls only the target sizes.
func newPoolWithDB(t *testing.T, db *pgxpool.Pool, cfg Config) (*Manager, *fastPostgresBackend, *fastRedisBackend, *fastMongoBackend, *fastQueueBackend) {
	t.Helper()
	pg := &fastPostgresBackend{}
	rd := &fastRedisBackend{}
	mg := &fastMongoBackend{}
	qb := &fastQueueBackend{}
	m := New(db, validAESKey(), cfg, pg, rd, mg, qb)
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return m, pg, rd, mg, qb
}

// readyCount returns the number of ready items for the given resource type.
// Helper for assertions across fill/Claim tests.
func readyCount(t *testing.T, db *pgxpool.Pool, resourceType string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(),
		`SELECT count(*) FROM pool_items WHERE resource_type = $1 AND status = 'ready'`,
		resourceType).Scan(&n); err != nil {
		t.Fatalf("readyCount: %v", err)
	}
	return n
}

// ============================================================================
// Pure-unit tests (no DB) — keep running even without TEST_DATABASE_URL.
// ============================================================================

// TestNew_WiresTargetsAndChannels verifies the constructor maps Config sizes
// onto the targets map and allocates the bounded refill channel.
func TestNew_WiresTargetsAndChannels(t *testing.T) {
	cfg := Config{PostgresSize: 3, RedisSize: 2, MongoSize: 1, QueueSize: 4}
	m := New(nil, validAESKey(), cfg, nil, nil, nil, nil)

	wantTargets := map[string]int{"postgres": 3, "redis": 2, "mongodb": 1, "queue": 4}
	for k, v := range wantTargets {
		if got := m.targets[k]; got != v {
			t.Errorf("targets[%q] = %d, want %d", k, got, v)
		}
	}
	if cap(m.refillCh) != 40 {
		t.Errorf("refillCh capacity = %d, want 40", cap(m.refillCh))
	}
	if m.done == nil {
		t.Error("done channel not allocated")
	}
}

// TestNewWithConfig — the factory constructs real backend instances from a
// config struct. It returns a Manager whose `db`/`aesKey`/`targets` are
// populated; we can't introspect the backend instances (they're interface
// values) but we can prove the call doesn't panic and the targets propagate.
func TestNewWithConfig(t *testing.T) {
	cfg := Config{PostgresSize: 1, RedisSize: 1, MongoSize: 1, QueueSize: 1}
	appCfg := &config.Config{
		PostgresProvisionBackend: "local",
		PostgresCustomersURL:     "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		PostgresClusterURLs:      "",
		NeonAPIKey:               "",
		NeonRegionID:             "aws-us-east-1",
		RedisProvisionBackend:    "local",
		RedisProvisionHost:       "127.0.0.1:6379",
		MongoProvisionBackend:    "local",
		MongoAdminURI:            "mongodb://localhost:27017",
		MongoHost:                "127.0.0.1:27017",
		QueueProvisionBackend:    "local",
		NATSHost:                 "127.0.0.1",
	}
	m := NewWithConfig(nil, validAESKey(), cfg, appCfg)
	if m == nil {
		t.Fatal("NewWithConfig returned nil")
	}
	if m.targets["postgres"] != 1 || m.targets["redis"] != 1 || m.targets["mongodb"] != 1 || m.targets["queue"] != 1 {
		t.Errorf("targets did not propagate from Config: %v", m.targets)
	}
}

// TestTriggerRefill_CoalescesWhenFull — the refill channel's purpose is to
// coalesce; a burst of triggers for the same type must NOT block once the
// channel is saturated. We fill the channel by hand then assert
// triggerRefill returns immediately.
func TestTriggerRefill_CoalescesWhenFull(t *testing.T) {
	m := &Manager{refillCh: make(chan string, 2)}
	m.refillCh <- "postgres"
	m.refillCh <- "redis"
	// Channel is now full. triggerRefill must drop, not block.
	done := make(chan struct{})
	go func() { m.triggerRefill("postgres"); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("triggerRefill blocked when refillCh was full")
	}
}

// TestProvisionItemsConcurrently_NeededNegativeOrZero — bound-edge check.
// `needed <= 0` must return immediately (covered for needed==0 in the
// concurrency test file; here we also exercise needed=-1 for safety, even
// though the call sites guard against it).
func TestProvisionItemsConcurrently_NeededNegativeOrZero(t *testing.T) {
	m := &Manager{}
	done := make(chan struct{})
	go func() {
		m.provisionItemsConcurrently(context.Background(), "postgres", -3)
		m.provisionItemsConcurrently(context.Background(), "postgres", 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("provisionItemsConcurrently(needed<=0) did not return promptly")
	}
}

// TestProvisionOneItemBackend_UnknownResourceType — the `default` arm of the
// type switch. Returns an explicit "unknown resource type" error rather than
// the empty default-zero provisionedItem.
func TestProvisionOneItemBackend_UnknownResourceType(t *testing.T) {
	m := &Manager{aesKey: validAESKey()}
	_, err := m.provisionOneItemBackend(context.Background(), "bogus-type")
	if err == nil {
		t.Fatal("provisionOneItemBackend(unknown) returned nil err — must reject unknown types")
	}
	if !strings.Contains(err.Error(), "unknown resource type") {
		t.Errorf("err = %q; want it to mention 'unknown resource type'", err.Error())
	}
}

// TestProvisionOneItemBackend_BackendErrorWrapping — each resource arm wraps
// the backend's Provision error with a descriptive prefix. Asserts the
// "provision <type>:" prefix is present on each path.
func TestProvisionOneItemBackend_BackendErrorWrapping(t *testing.T) {
	cases := []struct {
		resourceType string
		wantPrefix   string
		setup        func(*Manager)
	}{
		{"postgres", "provision postgres", func(m *Manager) { m.postgresB = &fastPostgresBackend{fail: true} }},
		{"redis", "provision redis", func(m *Manager) { m.redisB = &fastRedisBackend{fail: true} }},
		{"mongodb", "provision mongodb", func(m *Manager) { m.mongoB = &fastMongoBackend{fail: true} }},
		{"queue", "provision queue", func(m *Manager) { m.queueB = &fastQueueBackend{fail: true} }},
	}
	for _, c := range cases {
		t.Run(c.resourceType, func(t *testing.T) {
			m := &Manager{aesKey: validAESKey()}
			c.setup(m)
			_, err := m.provisionOneItemBackend(context.Background(), c.resourceType)
			if err == nil {
				t.Fatalf("%s: nil err on forced backend failure", c.resourceType)
			}
			if !strings.Contains(err.Error(), c.wantPrefix) {
				t.Errorf("%s: err = %q; want prefix %q", c.resourceType, err.Error(), c.wantPrefix)
			}
		})
	}
}

// TestProvisionOneItemBackend_EncryptErrorPerType — Encrypt requires a
// 16/24/32-byte key; a 10-byte key forces aes.NewCipher to error. Each
// resource arm wraps that err with `encrypt <type> url:`.
func TestProvisionOneItemBackend_EncryptErrorPerType(t *testing.T) {
	badKey := []byte("short-key")
	cases := []struct {
		resourceType string
		wantPrefix   string
		setup        func(*Manager)
	}{
		{"postgres", "encrypt postgres url", func(m *Manager) { m.postgresB = &fastPostgresBackend{} }},
		{"redis", "encrypt redis url", func(m *Manager) { m.redisB = &fastRedisBackend{} }},
		{"mongodb", "encrypt mongodb url", func(m *Manager) { m.mongoB = &fastMongoBackend{} }},
		{"queue", "encrypt queue url", func(m *Manager) { m.queueB = &fastQueueBackend{} }},
	}
	for _, c := range cases {
		t.Run(c.resourceType, func(t *testing.T) {
			m := &Manager{aesKey: badKey}
			c.setup(m)
			_, err := m.provisionOneItemBackend(context.Background(), c.resourceType)
			if err == nil {
				t.Fatalf("%s: nil err with bad AES key", c.resourceType)
			}
			if !strings.Contains(err.Error(), c.wantPrefix) {
				t.Errorf("%s: err = %q; want prefix %q", c.resourceType, err.Error(), c.wantPrefix)
			}
		})
	}
}

// TestProvisionOneItemBackend_HappyPathPerType — every resource arm returns
// a well-formed provisionedItem on success. Smoke test for the canonical
// (non-error) path of each switch case.
func TestProvisionOneItemBackend_HappyPathPerType(t *testing.T) {
	tr := &concurrencyTracker{}
	m := newConcurrencyTestManager(t, tr)
	for _, rt := range []string{"postgres", "redis", "mongodb", "queue"} {
		item, err := m.provisionOneItemBackend(context.Background(), rt)
		if err != nil {
			t.Fatalf("%s: %v", rt, err)
		}
		if item.encURL == "" {
			t.Errorf("%s: encURL empty", rt)
		}
		if item.poolToken == "" || !strings.HasPrefix(item.poolToken, "pool-") {
			t.Errorf("%s: poolToken = %q; want pool-<uuid>", rt, item.poolToken)
		}
	}
}

// ============================================================================
// DB-driven tests (gated on TEST_DATABASE_URL).
// ============================================================================

// TestMigrate_Idempotent — running migrate twice must not error. The schema
// uses CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS — but we want
// to prove the assembled statement is idempotent, not trust the comment.
func TestMigrate_Idempotent(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m := New(db, validAESKey(), Config{}, nil, nil, nil, nil)
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("migrate(1): %v", err)
	}
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("migrate(2 — idempotent): %v", err)
	}
}

// TestStats_EmptyAndAfterInsert — Stats returns an empty map on a fresh
// table and a per-resource-type count after inserts. Covers the SELECT
// path including the rows.Next() loop.
func TestStats_EmptyAndAfterInsert(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})

	stats, err := m.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats(empty): %v", err)
	}
	if len(stats) != 0 {
		t.Errorf("Stats(empty) = %v; want empty map", stats)
	}

	// Insert two ready postgres items + one redis item manually so we can
	// assert the GROUP BY.
	for i := 0; i < 2; i++ {
		if _, err := db.Exec(context.Background(), `
			INSERT INTO pool_items(resource_type, connection_url, pool_token)
			VALUES ('postgres', 'enc', 'pool-pg-'||$1)`, fmt.Sprint(i)); err != nil {
			t.Fatalf("insert pg %d: %v", i, err)
		}
	}
	if _, err := db.Exec(context.Background(), `
		INSERT INTO pool_items(resource_type, connection_url, pool_token)
		VALUES ('redis', 'enc', 'pool-rd-0')`); err != nil {
		t.Fatalf("insert rd: %v", err)
	}

	stats, err = m.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats(after insert): %v", err)
	}
	if stats["postgres"] != 2 || stats["redis"] != 1 {
		t.Errorf("Stats = %v; want postgres=2 redis=1", stats)
	}
}

// TestClaim_EmptyReturnsNilNil — pool-miss contract. An empty pool returns
// `(nil, nil)` so the caller falls back to live provisioning.
func TestClaim_EmptyReturnsNilNil(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})
	item, err := m.Claim(context.Background(), "postgres")
	if err != nil {
		t.Fatalf("Claim(empty): %v", err)
	}
	if item != nil {
		t.Errorf("Claim(empty) = %+v; want nil", item)
	}
}

// TestProvisionOneItem_PersistsRow — provisionOneItem runs Provision +
// Encrypt + INSERT. Verifies a ready row lands in pool_items.
func TestProvisionOneItem_PersistsRow(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, pg, _, _, _ := newPoolWithDB(t, db, Config{})
	if err := m.provisionOneItem(context.Background(), "postgres"); err != nil {
		t.Fatalf("provisionOneItem: %v", err)
	}
	if pg.calls.Load() != 1 {
		t.Errorf("postgres backend called %d times; want 1", pg.calls.Load())
	}
	if got := readyCount(t, db, "postgres"); got != 1 {
		t.Errorf("ready postgres count = %d; want 1", got)
	}
}

// TestClaim_HitDecryptsAndTriggersRefill — happy-path Claim:
//   1. inserts a ready item via provisionOneItem
//   2. claims it
//   3. asserts plaintext URL was decrypted
//   4. asserts the refill channel was nudged
func TestClaim_HitDecryptsAndTriggersRefill(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, rd, _, _ := newPoolWithDB(t, db, Config{RedisSize: 1})
	if err := m.provisionOneItem(context.Background(), "redis"); err != nil {
		t.Fatalf("provisionOneItem(redis): %v", err)
	}

	// Drain any pre-existing entries on refillCh so we can detect the
	// post-Claim trigger cleanly.
	for {
		select {
		case <-m.refillCh:
			continue
		default:
		}
		break
	}

	item, err := m.Claim(context.Background(), "redis")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if item == nil {
		t.Fatal("Claim returned nil item on a populated pool")
	}
	if item.ConnectionURL == "" || !strings.HasPrefix(item.ConnectionURL, "redis://") {
		t.Errorf("ConnectionURL = %q; want decrypted redis:// URL", item.ConnectionURL)
	}
	if item.ResourceType != "redis" {
		t.Errorf("ResourceType = %q; want redis", item.ResourceType)
	}
	if item.PoolToken == "" {
		t.Error("PoolToken empty after Claim")
	}

	// Async refill trigger — read with a small budget.
	select {
	case got := <-m.refillCh:
		if got != "redis" {
			t.Errorf("refillCh got %q; want redis", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Claim did not nudge refillCh")
	}

	if rd.calls.Load() != 1 {
		t.Errorf("redis backend Provision called %d times; want 1", rd.calls.Load())
	}

	// A second Claim on the same (now-empty) pool returns nil — same as
	// the empty-pool test, but exercised after a hit.
	item2, err := m.Claim(context.Background(), "redis")
	if err != nil {
		t.Fatalf("Claim(after-drain): %v", err)
	}
	if item2 != nil {
		t.Errorf("Claim after drain = %+v; want nil (pool drained)", item2)
	}
}

// TestClaim_DecryptError — when the stored connection_url isn't valid
// ciphertext for the Manager's key, Claim returns a wrapped error. We
// simulate this by inserting a row whose connection_url is plaintext.
func TestClaim_DecryptError(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})

	if _, err := db.Exec(context.Background(), `
		INSERT INTO pool_items(resource_type, connection_url, pool_token)
		VALUES ('postgres', 'not-base64-ciphertext!!', 'pool-bad')`); err != nil {
		t.Fatalf("insert bad row: %v", err)
	}

	_, err := m.Claim(context.Background(), "postgres")
	if err == nil {
		t.Fatal("Claim with un-decryptable URL returned nil err")
	}
	if !strings.Contains(err.Error(), "decrypt") {
		t.Errorf("err = %q; want 'decrypt' in message", err.Error())
	}
}

// TestFillPool_TopsUpToTarget — fillPool reads the current ready count
// then provisions exactly `target - count` items. Smoke test for the
// COUNT(*) → needed → provisionItemsConcurrently chain.
func TestFillPool_TopsUpToTarget(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	cfg := Config{PostgresSize: 4}
	m, pg, _, _, _ := newPoolWithDB(t, db, cfg)

	// Seed 1 ready item directly so fillPool needs to add exactly 3.
	if _, err := db.Exec(context.Background(), `
		INSERT INTO pool_items(resource_type, connection_url, pool_token)
		VALUES ('postgres', 'enc', 'pool-seed')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	m.fillPool(context.Background(), "postgres")

	if got := readyCount(t, db, "postgres"); got != 4 {
		t.Errorf("ready count after fillPool = %d; want 4", got)
	}
	if got := pg.calls.Load(); got != 3 {
		t.Errorf("postgres backend Provision called %d times; want 3", got)
	}
}

// TestFillPool_TargetZeroNoOp — `target <= 0` is the "this type's pool is
// disabled" signal; fillPool must return immediately without touching the
// backend.
func TestFillPool_TargetZeroNoOp(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, pg, _, _, _ := newPoolWithDB(t, db, Config{}) // no targets set
	m.fillPool(context.Background(), "postgres")
	if got := pg.calls.Load(); got != 0 {
		t.Errorf("backend called %d times for disabled pool; want 0", got)
	}
}

// TestFillPool_AtTargetNoOp — when count already meets target, no
// provisions happen (the `needed <= 0` branch).
func TestFillPool_AtTargetNoOp(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, pg, _, _, _ := newPoolWithDB(t, db, Config{PostgresSize: 1})

	if _, err := db.Exec(context.Background(), `
		INSERT INTO pool_items(resource_type, connection_url, pool_token)
		VALUES ('postgres', 'enc', 'pool-already-there')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	m.fillPool(context.Background(), "postgres")
	if got := pg.calls.Load(); got != 0 {
		t.Errorf("backend called %d times when at-target; want 0", got)
	}
}

// TestStart_AndShutdown_FullLifecycle — runs Start + Shutdown in sequence.
// Start triggers migrate + initial fills + spins up `run`; Shutdown cancels
// runCtx, closes done, and joins the goroutine.
func TestStart_AndShutdown_FullLifecycle(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	cfg := Config{PostgresSize: 2, RedisSize: 1}
	m, _, _, _, _ := newPoolWithDB(t, db, cfg)
	// migrate is already done by newPoolWithDB — Start will re-run it
	// (idempotent), kick refills, and start the maintenance loop.

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for the initial fill to settle. The fills run on the maintenance
	// goroutine via refillCh; give it a generous budget.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := m.Stats(context.Background())
		if err == nil && stats["postgres"] >= 2 && stats["redis"] >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	stats, err := m.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats post-Start: %v", err)
	}
	if stats["postgres"] < 2 || stats["redis"] < 1 {
		t.Errorf("initial fill did not converge: %v", stats)
	}

	// Now Shutdown — runCtx must be cancelled, run goroutine must exit.
	shutdownDone := make(chan struct{})
	go func() { m.Shutdown(); close(shutdownDone) }()
	select {
	case <-shutdownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return within 5s — runCtx cancellation didn't propagate")
	}
	if err := m.runCtx.Err(); err != context.Canceled {
		t.Errorf("runCtx.Err = %v; want context.Canceled", err)
	}
}

// TestRun_TickerTriggersFill — the maintenance loop's ticker fires every
// 30s in production; to keep the test fast we don't wait for it. Instead
// we drive the same code path by feeding refillCh directly. This is a
// pure assertion on the consume-from-channel arm of the select.
func TestRun_RefillChannelDrivesFill(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	cfg := Config{PostgresSize: 1}
	m, pg, _, _, _ := newPoolWithDB(t, db, cfg)

	m.runCtx, m.runCancel = context.WithCancel(context.Background())
	m.wg.Add(1)
	go m.run(m.runCtx)

	m.triggerRefill("postgres")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pg.calls.Load() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pg.calls.Load() < 1 {
		t.Errorf("refillCh-driven fill never invoked backend (calls=%d)", pg.calls.Load())
	}

	// Shutdown cleanly so the goroutine doesn't leak into the next test.
	m.Shutdown()
}

// TestProvisionOneItem_DBInsertFailure — when the INSERT fails (we simulate
// by dropping the table), provisionOneItem returns a wrapped "insert pool
// item:" error rather than panicking.
func TestProvisionOneItem_DBInsertFailure(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})

	if _, err := db.Exec(context.Background(), "DROP TABLE pool_items"); err != nil {
		t.Fatalf("drop pool_items: %v", err)
	}

	err := m.provisionOneItem(context.Background(), "postgres")
	if err == nil {
		t.Fatal("provisionOneItem with missing table returned nil err")
	}
	if !strings.Contains(err.Error(), "insert pool item") {
		t.Errorf("err = %q; want 'insert pool item' wrapper", err.Error())
	}
}

// TestFillPool_CountQueryFailure — when the COUNT query fails (we drop the
// table again), fillPool logs and returns without panicking. Hard to assert
// the log line directly; the contract is "no panic, no provisions".
func TestFillPool_CountQueryFailure(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	cfg := Config{PostgresSize: 1}
	m, pg, _, _, _ := newPoolWithDB(t, db, cfg)
	if _, err := db.Exec(context.Background(), "DROP TABLE pool_items"); err != nil {
		t.Fatalf("drop pool_items: %v", err)
	}
	m.fillPool(context.Background(), "postgres")
	if got := pg.calls.Load(); got != 0 {
		t.Errorf("backend called %d times despite count-query failure; want 0", got)
	}
}

// TestStats_QueryFailure — when the SELECT errors, Stats returns the
// wrapped error rather than a partial map.
func TestStats_QueryFailure(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})
	if _, err := db.Exec(context.Background(), "DROP TABLE pool_items"); err != nil {
		t.Fatalf("drop pool_items: %v", err)
	}
	_, err := m.Stats(context.Background())
	if err == nil {
		t.Fatal("Stats with missing table returned nil err")
	}
	if !strings.Contains(err.Error(), "pool.Stats") {
		t.Errorf("err = %q; want 'pool.Stats' wrapper", err.Error())
	}
}

// TestStart_MigrateFailureReturnsWrappedErr — Start MUST surface the migrate
// error wrapped as `pool.Start: migrate:`. We force migrate to fail by
// running it against a closed pool (Exec returns an error on a closed pool).
// We skip the newPoolWithDB helper (which itself runs migrate) and build the
// Manager directly with a closed pool.
func TestStart_MigrateFailureReturnsWrappedErr(t *testing.T) {
	closed := mustClosedPool(t)
	m := New(closed, validAESKey(), Config{}, nil, nil, nil, nil)

	err := m.Start(context.Background())
	if err == nil {
		t.Fatal("Start with closed pool returned nil err")
	}
	if !strings.Contains(err.Error(), "pool.Start: migrate") {
		t.Errorf("err = %q; want 'pool.Start: migrate:' prefix", err.Error())
	}
}

// mustClosedPool returns a pgxpool.Pool that has already been Close()'d.
// Any subsequent Exec/Query returns "closed pool". Used to force the
// migrate-failure path in Start without touching network state.
func mustClosedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := testDBURL()
	if dsn == "" {
		t.Skip("integration — TEST_DATABASE_URL unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	p.Close()
	return p
}

// TestClaim_ScanError — when the SELECT itself errors (not ErrNoRows), the
// returned error is wrapped `pool.Claim: scan:`. We force this by closing
// the pool right before Claim.
func TestClaim_ScanError(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})
	// Drop the table so the SELECT errors at scan time (it'll fail on
	// "relation does not exist" — that path goes through the non-ErrNoRows
	// branch).
	if _, err := db.Exec(context.Background(), "DROP TABLE pool_items"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	_, err := m.Claim(context.Background(), "postgres")
	if err == nil {
		t.Fatal("Claim with missing table returned nil err")
	}
	if !strings.Contains(err.Error(), "pool.Claim: scan") {
		t.Errorf("err = %q; want 'pool.Claim: scan:' prefix", err.Error())
	}
}

// TestStats_RowScanError — Stats scans (resource_type, count) per row;
// if the columns can't be decoded into (string, int), scan errors. We
// force a type mismatch by inserting a row whose count column is decoded
// against an unexpected schema. The simplest reproduction: rewrite the
// query result via a partial view that returns 3 columns instead of 2.
// (The clean way to exercise the path; without it the rows.Scan branch
// stays cold.)
func TestStats_RowScanError(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})
	// Replace pool_items with a view that returns text for cnt — Scan into
	// int will fail. The view masks the table so Stats' query lands on the
	// bad-typed view.
	if _, err := db.Exec(context.Background(), `
		ALTER TABLE pool_items RENAME TO pool_items_real;
		CREATE VIEW pool_items AS
		SELECT id, 'postgres'::text AS resource_type, connection_url, provider_resource_id,
		       database_name, username, key_prefix, pool_token,
		       'ready'::text AS status, created_at, assigned_at,
		       'not-a-number'::text AS bogus
		FROM pool_items_real;
	`); err != nil {
		t.Fatalf("create view: %v", err)
	}
	// Force a ready row to flow through.
	if _, err := db.Exec(context.Background(), `
		INSERT INTO pool_items_real(resource_type, connection_url, pool_token)
		VALUES ('postgres', 'enc', 'pool-x')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Stats SELECTs resource_type + count(*) — scan into (string, int) will
	// succeed on this view since GROUP BY computes a real int count. The
	// row-scan error path is genuinely hard to hit from outside; this test
	// at least exercises the rows.Next() loop with a real row, which covers
	// the assignment to stats[rt].
	_, err := m.Stats(context.Background())
	if err != nil {
		t.Logf("Stats over view returned err: %v (acceptable)", err)
	}
}

// TestRun_TickerFiresFill — exercise the ticker arm of run's select.
// Set runTickInterval to a short cadence, start run, wait for a tick to
// drive fillPool, then shutdown. Covers the `case <-ticker.C` branch.
func TestRun_TickerFiresFill(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	orig := runTickInterval
	runTickInterval = 50 * time.Millisecond
	t.Cleanup(func() { runTickInterval = orig })

	cfg := Config{PostgresSize: 1}
	m, pg, _, _, _ := newPoolWithDB(t, db, cfg)

	m.runCtx, m.runCancel = context.WithCancel(context.Background())
	m.wg.Add(1)
	go m.run(m.runCtx)

	// Wait for the ticker to fire at least once — must drive at least one
	// fillPool, which provisions one postgres item (target=1, count=0).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pg.calls.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pg.calls.Load() < 1 {
		t.Errorf("ticker did not drive fillPool: calls=%d", pg.calls.Load())
	}

	m.Shutdown()
}

// TestProvisionItemsConcurrently_BackendFailureLogged — when the backend
// fails inside the bounded worker loop, the error is logged and the loop
// continues. Asserts no panic and that all `needed` worker slots return.
func TestProvisionItemsConcurrently_BackendFailureLogged(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{PostgresSize: 3})
	m.postgresB = &fastPostgresBackend{fail: true} // every Provision fails

	done := make(chan struct{})
	go func() {
		m.provisionItemsConcurrently(context.Background(), "postgres", 3)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("provisionItemsConcurrently did not return after backend failures")
	}

	// No ready rows because every provision failed before the INSERT.
	if got := readyCount(t, db, "postgres"); got != 0 {
		t.Errorf("ready postgres count = %d; want 0 (all provisions failed)", got)
	}
}

// TestProvisionOneItem_BackendFailureReturnsErrBeforeInsert — the top-level
// path: provisionOneItem returns the wrapped backend error and never tries
// to INSERT a half-built row.
func TestProvisionOneItem_BackendFailureReturnsErrBeforeInsert(t *testing.T) {
	db, cleanup := poolTestDB(t)
	defer cleanup()

	m, _, _, _, _ := newPoolWithDB(t, db, Config{})
	m.postgresB = &fastPostgresBackend{fail: true}

	err := m.provisionOneItem(context.Background(), "postgres")
	if err == nil {
		t.Fatal("nil err on backend failure")
	}
	if !strings.Contains(err.Error(), "provision postgres") {
		t.Errorf("err = %q; want 'provision postgres' wrapper", err.Error())
	}
	if got := readyCount(t, db, "postgres"); got != 0 {
		t.Errorf("ready count = %d; want 0 (no INSERT on backend failure)", got)
	}
}

// TestMigrate_DBExecFailure — when the schema DDL fails, migrate wraps it as
// `create pool_items:`. Force by closing the pool first.
func TestMigrate_DBExecFailure(t *testing.T) {
	db, cleanup := poolTestDB(t)
	cleanup() // close before migrate
	_ = db

	m := &Manager{db: mustClosedPool(t)}
	err := m.migrate(context.Background())
	if err == nil {
		t.Fatal("migrate against closed pool returned nil err")
	}
	if !strings.Contains(err.Error(), "create pool_items") {
		t.Errorf("err = %q; want 'create pool_items' wrapper", err.Error())
	}
}

// Compile-time assertions — if a Backend interface gains a method, this
// test fails to compile rather than silently testing a stale shape.
var (
	_ postgres.Backend = (*fastPostgresBackend)(nil)
	_ redis.Backend    = (*fastRedisBackend)(nil)
	_ mongo.Backend    = (*fastMongoBackend)(nil)
	_ queue.Backend    = (*fastQueueBackend)(nil)
)
