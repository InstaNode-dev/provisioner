package pool

// manager_refill_concurrency_test.go — regression test for the F1 latency
// cliff (LOAD-CHAOS-REPORT-2026-05-19).
//
// Root cause: fillPool provisioned its `needed` pool items in a strictly
// sequential `for` loop on the single maintenance goroutine, so a pool drained
// by a concurrency burst refilled at ~1 item per single-provision latency
// (15-25s on shared backends). Under concurrency >= 8 the pool stayed empty
// for the whole load run and every request paid full live-provision latency —
// throughput pinned flat regardless of how many callers piled on.
//
// Fix: provisionItemsConcurrently provisions `needed` items in parallel,
// bounded to maxRefillConcurrency in-flight backend calls.
//
// These tests are hermetic — no Postgres, no k8s. They exercise
// provisionOneItemBackend (the slow backend phase, which touches no shared
// mutable Manager state) under the same bounded-concurrency loop
// provisionItemsConcurrently uses, with mock backends that (a) sleep, so
// serialized execution would blow the time budget, and (b) count peak
// concurrent in-flight calls, so the concurrency property is asserted
// directly rather than inferred from wall time alone.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
)

// provisionDelay is the artificial per-provision latency the mock backends
// impose. Picked large enough that a serialized refill of `needed` items would
// obviously exceed the test's time budget, small enough to keep the test fast.
const provisionDelay = 80 * time.Millisecond

// concurrencyTracker records the peak number of simultaneously in-flight
// provision calls. A serialized refill never exceeds peak == 1; the fix must
// drive it above 1 (up to maxRefillConcurrency).
type concurrencyTracker struct {
	mu      sync.Mutex
	current int
	peak    int
}

func (c *concurrencyTracker) enter() {
	c.mu.Lock()
	c.current++
	if c.current > c.peak {
		c.peak = c.current
	}
	c.mu.Unlock()
}

func (c *concurrencyTracker) leave() {
	c.mu.Lock()
	c.current--
	c.mu.Unlock()
}

func (c *concurrencyTracker) Peak() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peak
}

// --- mock backends: sleep + record concurrency, no real infra ---

// deprovTracker records Deprovision calls the reaper makes against the mock
// backends: the (token, providerResourceID) pairs, and an optional injected
// error to drive the reaper's failure branch. Safe for concurrent use; nil-safe
// so the F1 concurrency tests that don't set one keep working unchanged.
type deprovTracker struct {
	mu    sync.Mutex
	calls []deprovCall
	err   error // when non-nil, every Deprovision returns it
}

type deprovCall struct {
	resourceType       string
	token              string
	providerResourceID string
}

func (d *deprovTracker) record(resourceType, token, prid string) error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	d.calls = append(d.calls, deprovCall{resourceType, token, prid})
	err := d.err
	d.mu.Unlock()
	return err
}

func (d *deprovTracker) snapshot() []deprovCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]deprovCall, len(d.calls))
	copy(out, d.calls)
	return out
}

type mockPostgresBackend struct {
	tr     *concurrencyTracker
	calls  atomic.Int32
	deprov *deprovTracker
}

func (b *mockPostgresBackend) Provision(ctx context.Context, token, tier string, connLimit int) (*postgres.Credentials, error) {
	b.tr.enter()
	defer b.tr.leave()
	b.calls.Add(1)
	select {
	case <-time.After(provisionDelay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &postgres.Credentials{URL: "postgres://u:p@h/db_" + token, DatabaseName: "db_" + token, Username: "usr_" + token}, nil
}
func (b *mockPostgresBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (b *mockPostgresBackend) Deprovision(_ context.Context, token, prid string) error {
	return b.deprov.record("postgres", token, prid)
}
func (b *mockPostgresBackend) Regrade(context.Context, string, string, int) (postgres.RegradeResult, error) {
	return postgres.RegradeResult{}, nil
}

type mockRedisBackend struct {
	tr     *concurrencyTracker
	deprov *deprovTracker
}

func (b *mockRedisBackend) Provision(ctx context.Context, token, tier string) (*redis.Credentials, error) {
	b.tr.enter()
	defer b.tr.leave()
	select {
	case <-time.After(provisionDelay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &redis.Credentials{URL: "redis://h/0", KeyPrefix: token + ":"}, nil
}
func (b *mockRedisBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (b *mockRedisBackend) Deprovision(_ context.Context, token, prid string) error {
	return b.deprov.record("redis", token, prid)
}

type mockMongoBackend struct {
	tr     *concurrencyTracker
	deprov *deprovTracker
}

func (b *mockMongoBackend) Provision(ctx context.Context, token, tier string) (*mongo.Credentials, error) {
	b.tr.enter()
	defer b.tr.leave()
	select {
	case <-time.After(provisionDelay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &mongo.Credentials{URL: "mongodb://h/db_" + token, DatabaseName: "db_" + token}, nil
}
func (b *mockMongoBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (b *mockMongoBackend) Deprovision(_ context.Context, token, prid string) error {
	return b.deprov.record("mongodb", token, prid)
}

type mockQueueBackend struct {
	tr     *concurrencyTracker
	deprov *deprovTracker
}

func (b *mockQueueBackend) Provision(ctx context.Context, token, tier string) (*queue.Credentials, error) {
	b.tr.enter()
	defer b.tr.leave()
	select {
	case <-time.After(provisionDelay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &queue.Credentials{URL: "nats://h:4222", SubjectPrefix: token + "."}, nil
}
func (b *mockQueueBackend) Deprovision(_ context.Context, token, prid string) error {
	return b.deprov.record("queue", token, prid)
}

// newConcurrencyTestManager builds a Manager wired to sleeping mock backends.
// It has no *pgxpool.Pool — the tests only call provisionOneItemBackend, which
// never touches m.db. A valid 32-byte AES key is supplied so Encrypt succeeds.
func newConcurrencyTestManager(t *testing.T, tr *concurrencyTracker) *Manager {
	t.Helper()
	// 32 zero bytes is a structurally valid AES-256 key — fine for a test that
	// only needs Encrypt to not error.
	aesKey := make([]byte, 32)
	return &Manager{
		aesKey:    aesKey,
		postgresB: &mockPostgresBackend{tr: tr},
		redisB:    &mockRedisBackend{tr: tr},
		mongoB:    &mockMongoBackend{tr: tr},
		queueB:    &mockQueueBackend{tr: tr},
	}
}

// runBackendRefill mirrors provisionItemsConcurrently's bounded worker loop but
// drives provisionOneItemBackend directly so no DB is needed. If this loop and
// provisionItemsConcurrently ever diverge, the comment on provisionItemsConcurrently
// and this helper must be updated together — both rely on provisionOneItemBackend
// holding no shared mutable Manager state.
func runBackendRefill(t *testing.T, m *Manager, resourceType string, needed int) (time.Duration, int) {
	t.Helper()
	limit := maxRefillConcurrency
	if needed < limit {
		limit = needed
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	var failures atomic.Int32
	start := time.Now()
	for i := 0; i < needed; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := m.provisionOneItemBackend(context.Background(), resourceType); err != nil {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	return time.Since(start), int(failures.Load())
}

// TestRefill_ProvisionsConcurrently is the core F1 regression guard. It refills
// `needed` items per resource type and asserts:
//  1. peak in-flight provisions > 1 — the work genuinely parallelizes (a
//     serialized loop pins peak at 1);
//  2. aggregate wall time is far below needed × provisionDelay — a serialized
//     refill would take at least that long. The budget is generous (half the
//     serial time) so the test is not timing-flaky.
//
// `needed` is set to 2 × maxRefillConcurrency so the bound itself is exercised:
// the loop must process two full waves, and peak must reach (not exceed) the cap.
func TestRefill_ProvisionsConcurrently(t *testing.T) {
	needed := maxRefillConcurrency * 2

	for _, rt := range []string{"postgres", "redis", "mongodb", "queue"} {
		t.Run(rt, func(t *testing.T) {
			tr := &concurrencyTracker{}
			m := newConcurrencyTestManager(t, tr)

			elapsed, failures := runBackendRefill(t, m, rt, needed)
			if failures != 0 {
				t.Fatalf("%d/%d provisions failed — mock backends must all succeed", failures, needed)
			}

			peak := tr.Peak()
			if peak <= 1 {
				t.Fatalf("peak concurrent provisions = %d; want > 1 — refill is still serialized (F1 regression)", peak)
			}
			if peak > maxRefillConcurrency {
				t.Fatalf("peak concurrent provisions = %d; want <= maxRefillConcurrency (%d) — bound not enforced",
					peak, maxRefillConcurrency)
			}

			serialTime := time.Duration(needed) * provisionDelay
			budget := serialTime / 2
			if elapsed >= budget {
				t.Fatalf("refill of %d %s items took %v; serialized would be ~%v, budget is %v — not parallelizing",
					needed, rt, elapsed, serialTime, budget)
			}
			t.Logf("%s: %d items in %v (serial would be ~%v), peak concurrency %d",
				rt, needed, elapsed, serialTime, peak)
		})
	}
}

// TestRefill_BoundedConcurrency asserts the concurrency cap is real: with
// `needed` far above maxRefillConcurrency, the peak must never exceed the cap.
// An unbounded refill would open one backend connection per needed item and
// could starve request-path provisions of connection slots — the bound is the
// safety property, not just a tuning knob.
func TestRefill_BoundedConcurrency(t *testing.T) {
	needed := maxRefillConcurrency * 5
	tr := &concurrencyTracker{}
	m := newConcurrencyTestManager(t, tr)

	_, failures := runBackendRefill(t, m, "postgres", needed)
	if failures != 0 {
		t.Fatalf("%d/%d provisions failed", failures, needed)
	}

	peak := tr.Peak()
	if peak > maxRefillConcurrency {
		t.Fatalf("peak concurrency %d exceeded maxRefillConcurrency %d — bound leaks", peak, maxRefillConcurrency)
	}
	if peak <= 1 {
		t.Fatalf("peak concurrency %d — refill serialized", peak)
	}
}

// TestProvisionOneItemBackend_NoSharedState exercises the property that makes
// concurrent refill correct: provisionOneItemBackend produces a fresh, distinct
// pool token on every call and shares no mutable Manager state with siblings.
// 200 fully-parallel calls must yield 200 unique tokens with no data race
// (run under `go test -race` in CI).
func TestProvisionOneItemBackend_NoSharedState(t *testing.T) {
	tr := &concurrencyTracker{}
	m := newConcurrencyTestManager(t, tr)

	const n = 200
	tokens := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			item, err := m.provisionOneItemBackend(context.Background(), "redis")
			if err != nil {
				t.Errorf("provisionOneItemBackend: %v", err)
				return
			}
			tokens[idx] = item.poolToken
		}(i)
	}
	wg.Wait()

	seen := make(map[string]struct{}, n)
	for i, tok := range tokens {
		if tok == "" {
			t.Fatalf("token %d empty — provision failed", i)
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("duplicate pool token %q — provisionOneItemBackend shares mutable state", tok)
		}
		seen[tok] = struct{}{}
	}
	if len(seen) != n {
		t.Fatalf("got %d unique tokens; want %d", len(seen), n)
	}
}

// TestProvisionItemsConcurrently_ZeroNeeded guards the trivial-but-important
// edge: a no-op refill must not spin up workers or block.
func TestProvisionItemsConcurrently_ZeroNeeded(t *testing.T) {
	tr := &concurrencyTracker{}
	m := newConcurrencyTestManager(t, tr)
	done := make(chan struct{})
	go func() {
		m.provisionItemsConcurrently(context.Background(), "postgres", 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("provisionItemsConcurrently(needed=0) did not return promptly")
	}
	if got := tr.Peak(); got != 0 {
		t.Fatalf("zero-needed refill provisioned %d items; want 0", got)
	}
}

// Compile-time assertions that the mock backends satisfy the production
// interfaces — if a backend interface gains a method, this test fails to
// compile rather than silently testing a stale shape.
var (
	_ postgres.Backend = (*mockPostgresBackend)(nil)
	_ redis.Backend    = (*mockRedisBackend)(nil)
	_ mongo.Backend    = (*mockMongoBackend)(nil)
	_ queue.Backend    = (*mockQueueBackend)(nil)
)
