package pool

// manager_db_errors_test.go — error-path coverage for the DB-touching seams
// that the happy-path integration tests don't exercise: Claim's decrypt
// failure, Stats / fillPool count-query failure (closed pool), and the queue
// resource type through provisionItemsConcurrently. Same TEST_PROVISIONER_-
// DATABASE_URL gate as manager_db_test.go.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestClaim_DecryptFailure — a ready row whose connection_url is not valid
// ciphertext must make Claim return a decrypt error (not panic, not a bogus
// item). crypto.Decrypt fails-open in the resource read path elsewhere, but in
// pool.Claim a malformed stored URL is a hard error — surface it.
func TestClaim_DecryptFailure(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{})

	// Insert a ready row directly with a connection_url that is not a valid
	// AES-GCM ciphertext for m.aesKey.
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO pool_items (resource_type, connection_url, pool_token, status)
		VALUES ('postgres', 'not-valid-ciphertext', 'pool-bad', 'ready')
	`); err != nil {
		t.Fatalf("seed bad row: %v", err)
	}

	item, err := m.Claim(context.Background(), "postgres")
	if err == nil {
		t.Fatalf("Claim should error on undecryptable url; got item=%+v", item)
	}
	if item != nil {
		t.Fatalf("Claim returned a non-nil item on decrypt failure: %+v", item)
	}
}

// TestStats_QueryError — Stats over a closed pool must return an error rather
// than an empty map. Exercises the fmt.Errorf("pool.Stats: ...") arm.
func TestStats_QueryError(t *testing.T) {
	dsn := testDSN(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	m := New(pool, make([]byte, 32), Config{}, nil, nil, nil, nil)
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool.Close() // now every query fails

	if _, err := m.Stats(context.Background()); err == nil {
		t.Fatal("Stats over a closed pool should return an error")
	}
}

// TestFillPool_CountError — fillPool over a closed pool must hit the count-query
// error arm and return without provisioning (and without panicking).
func TestFillPool_CountError(t *testing.T) {
	dsn := testDSN(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	tr := &concurrencyTracker{}
	m := New(pool, make([]byte, 32), Config{PostgresSize: 2},
		&mockPostgresBackend{tr: tr}, &mockRedisBackend{tr: tr},
		&mockMongoBackend{tr: tr}, &mockQueueBackend{tr: tr})
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool.Close()

	// Must return cleanly (count query errors, logs, returns) — no provision.
	m.fillPool(context.Background(), "postgres")
	if tr.Peak() != 0 {
		t.Fatalf("fillPool provisioned %d items despite count error; want 0", tr.Peak())
	}
}

// TestFillPool_QueueType — drives the queue arm of provisionOneItemBackend +
// the INSERT through fillPool against a real DB, closing the last
// provisionOneItemBackend switch branch the postgres/redis/mongo tests miss.
func TestFillPool_QueueType(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{QueueSize: 2})
	m.fillPool(context.Background(), "queue")
	if got := readyCount(t, pool, "queue"); got != 2 {
		t.Fatalf("ready queue count = %d, want 2", got)
	}
}

// TestStart_PeriodicTickRefill — Start launches the maintenance loop whose
// ticker branch periodically tops up every type. We can't wait the real 30s
// tick, but we can prove the loop is alive by triggering a refill via Claim's
// async triggerRefill and confirming the pool re-fills after a claim drains it.
// This exercises the run() refillCh arm beyond the single initial fill.
func TestStart_PeriodicTickRefill(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{RedisSize: 2})
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(m.Shutdown)

	// Wait for initial fill to reach target.
	waitFor(t, func() bool { return readyCount(t, pool, "redis") == 2 }, 5*time.Second,
		"initial refill never reached target 2")

	// Claim one — this fires triggerRefill, so run()'s refillCh arm tops it back up.
	item, err := m.Claim(context.Background(), "redis")
	if err != nil || item == nil {
		t.Fatalf("Claim: item=%v err=%v", item, err)
	}

	// The async refill triggered by Claim must restore the target.
	waitFor(t, func() bool { return readyCount(t, pool, "redis") == 2 }, 5*time.Second,
		"pool did not re-fill to target after a claim drained it")
}

// TestRun_PeriodicTickFills — the maintenance loop's ticker.C arm must top up
// every type without an explicit triggerRefill. We set a tiny tickInterval,
// drain the pool by deleting its ready rows out-of-band (no triggerRefill
// fired), and assert the periodic tick refills it back to target.
func TestRun_PeriodicTickFills(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{RedisSize: 2})
	m.tickInterval = 30 * time.Millisecond // fast ticks for the test

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(m.Shutdown)

	waitFor(t, func() bool { return readyCount(t, pool, "redis") == 2 }, 5*time.Second,
		"initial refill never reached target")

	// Drain out-of-band: delete ready rows directly so NO triggerRefill is
	// queued. Only the periodic ticker can bring the pool back.
	if _, err := pool.Exec(context.Background(),
		`DELETE FROM pool_items WHERE resource_type='redis'`); err != nil {
		t.Fatalf("drain: %v", err)
	}

	waitFor(t, func() bool { return readyCount(t, pool, "redis") == 2 }, 5*time.Second,
		"periodic ticker did not re-fill the pool after an out-of-band drain")
}

// waitFor polls cond until true or the deadline, failing with msg on timeout.
func waitFor(t *testing.T, cond func() bool, d time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
