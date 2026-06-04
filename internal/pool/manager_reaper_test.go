package pool

// manager_reaper_test.go — coverage for the hot-pool reaper (sweep #8).
//
// Two layers:
//
//   - Hermetic unit tests (no Postgres) for deprovisionBacking's resource-type
//     routing + unknown-type error. These run everywhere.
//   - DB-gated integration tests for reapFailed / reportStuckAssigned, which
//     need the real pool_items table (skip without TEST_PROVISIONER_DATABASE_URL).
//     They prove the load-bearing safety properties:
//       * an orphaned 'failed' item past failedReapGrace IS deprovisioned + deleted;
//       * a fresh 'failed' item (inside grace) is NOT touched;
//       * an 'assigned' item is NEVER deprovisioned (truehomie incident class),
//         only surfaced;
//       * a Deprovision failure leaves the row for the next tick (no orphaned infra).

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// --- hermetic: deprovisionBacking routing ---

// newRoutingManager builds a Manager wired only with mock backends that share a
// deprov tracker — no DB. deprovisionBacking touches no m.db, so this is enough.
func newRoutingManager(d *deprovTracker) *Manager {
	return &Manager{
		postgresB: &mockPostgresBackend{deprov: d},
		redisB:    &mockRedisBackend{deprov: d},
		mongoB:    &mockMongoBackend{deprov: d},
		queueB:    &mockQueueBackend{deprov: d},
	}
}

// TestDeprovisionBacking_RoutesByType asserts each resource type dispatches to
// its own backend with the pool_token passed through verbatim as the naming
// token (the property reapFailed relies on to target db_pool-<uuid> infra).
func TestDeprovisionBacking_RoutesByType(t *testing.T) {
	for _, rt := range []string{"postgres", "redis", "mongodb", "queue"} {
		t.Run(rt, func(t *testing.T) {
			d := &deprovTracker{}
			m := newRoutingManager(d)
			if err := m.deprovisionBacking(context.Background(), rt, "pool-tok-"+rt, "local:0"); err != nil {
				t.Fatalf("deprovisionBacking(%s): %v", rt, err)
			}
			calls := d.snapshot()
			if len(calls) != 1 {
				t.Fatalf("got %d Deprovision calls; want 1", len(calls))
			}
			c := calls[0]
			if c.resourceType != rt {
				t.Errorf("dispatched to backend %q; want %q", c.resourceType, rt)
			}
			if c.token != "pool-tok-"+rt {
				t.Errorf("token = %q; want pool token passed through verbatim", c.token)
			}
			if c.providerResourceID != "local:0" {
				t.Errorf("providerResourceID = %q; want local:0", c.providerResourceID)
			}
		})
	}
}

// TestDeprovisionBacking_UnknownType returns an error and calls no backend.
func TestDeprovisionBacking_UnknownType(t *testing.T) {
	d := &deprovTracker{}
	m := newRoutingManager(d)
	if err := m.deprovisionBacking(context.Background(), "elasticsearch", "tok", ""); err == nil {
		t.Fatal("expected error for unknown resource type")
	}
	if len(d.snapshot()) != 0 {
		t.Fatalf("unknown type called a backend; want none")
	}
}

// TestDeprovisionBacking_PropagatesError surfaces the backend error so reapFailed
// leaves the row in place for the next tick.
func TestDeprovisionBacking_PropagatesError(t *testing.T) {
	d := &deprovTracker{err: errors.New("backend down")}
	m := newRoutingManager(d)
	if err := m.deprovisionBacking(context.Background(), "redis", "tok", ""); err == nil {
		t.Fatal("expected backend error to propagate")
	}
}

// --- DB-gated: reapFailed / reportStuckAssigned ---

// newDBManagerWithDeprov mirrors newDBManager but threads a shared deprov
// tracker through every mock backend so the reaper's Deprovision calls are
// observable.
func newDBManagerWithDeprov(t *testing.T, cfg Config, d *deprovTracker) (*Manager, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_PROVISIONER_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_PROVISIONER_DATABASE_URL not set — skipping pool reaper DB integration tests")
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

	aesKey := make([]byte, 32)
	m := New(pool, aesKey, cfg,
		&mockPostgresBackend{deprov: d},
		&mockRedisBackend{deprov: d},
		&mockMongoBackend{deprov: d},
		&mockQueueBackend{deprov: d},
	)
	if err := m.migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE pool_items`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return m, pool
}

// seedItem inserts a pool_items row with an explicit status and assigned_at age.
// ageBack subtracts from now() so the row can be placed inside or past a grace.
func seedItem(t *testing.T, pool *pgxpool.Pool, rt, status, poolToken string, ageBack time.Duration) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO pool_items (resource_type, connection_url, provider_resource_id, pool_token, status, assigned_at)
		VALUES ($1, 'enc-url', 'local:0', $2, $3, now() - $4::interval)
		RETURNING id
	`, rt, poolToken, status, ageBack.String()).Scan(&id); err != nil {
		t.Fatalf("seed %s/%s item: %v", rt, status, err)
	}
	return id
}

func rowExists(t *testing.T, pool *pgxpool.Pool, id string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pool_items WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count by id: %v", err)
	}
	return n == 1
}

// TestReapFailed_ReapsOrphanedPastGrace is the core safety+leak test: a 'failed'
// item older than failedReapGrace is deprovisioned (with its pool_token) AND its
// row deleted, while a fresh 'failed' item inside the grace is left untouched.
func TestReapFailed_ReapsOrphanedPastGrace(t *testing.T) {
	d := &deprovTracker{}
	m, pool := newDBManagerWithDeprov(t, Config{}, d)
	ctx := context.Background()

	staleID := seedItem(t, pool, "postgres", "failed", "pool-stale", failedReapGrace+time.Hour)
	freshID := seedItem(t, pool, "postgres", "failed", "pool-fresh", time.Minute) // well inside grace

	m.reapFailed(ctx)

	if rowExists(t, pool, staleID) {
		t.Error("stale failed item past grace was NOT reaped — leak persists")
	}
	if !rowExists(t, pool, freshID) {
		t.Error("fresh failed item inside grace was reaped — grace window not honoured")
	}

	calls := d.snapshot()
	if len(calls) != 1 {
		t.Fatalf("got %d Deprovision calls; want exactly 1 (only the stale item)", len(calls))
	}
	if calls[0].token != "pool-stale" {
		t.Errorf("deprovisioned token = %q; want the stale item's pool_token", calls[0].token)
	}
}

// TestReapFailed_DeprovisionErrorLeavesRow proves a Deprovision failure does NOT
// delete the row — so the backing infra is never orphaned and the next tick
// retries.
func TestReapFailed_DeprovisionErrorLeavesRow(t *testing.T) {
	d := &deprovTracker{err: errors.New("backend unreachable")}
	m, pool := newDBManagerWithDeprov(t, Config{}, d)
	ctx := context.Background()

	id := seedItem(t, pool, "redis", "failed", "pool-x", failedReapGrace+time.Hour)

	m.reapFailed(ctx)

	if !rowExists(t, pool, id) {
		t.Error("row deleted despite Deprovision failure — infra would be orphaned")
	}
	if len(d.snapshot()) != 1 {
		t.Fatalf("expected exactly 1 Deprovision attempt, got %d", len(d.snapshot()))
	}
}

// TestReapStale_NeverDeprovisionsAssigned is the truehomie-incident guard: an
// 'assigned' item, however old, must never be deprovisioned by the provisioner
// (a bound item would be live customer infra). reapStale only surfaces it.
func TestReapStale_NeverDeprovisionsAssigned(t *testing.T) {
	d := &deprovTracker{}
	m, pool := newDBManagerWithDeprov(t, Config{PostgresSize: 1}, d)
	ctx := context.Background()

	id := seedItem(t, pool, "postgres", "assigned", "pool-bound", stuckAssignedGrace+24*time.Hour)

	m.reapStale(ctx)

	if !rowExists(t, pool, id) {
		t.Fatal("assigned item was deleted — provisioner must never reap assigned rows")
	}
	if n := len(d.snapshot()); n != 0 {
		t.Fatalf("assigned item triggered %d Deprovision calls; want 0 (would destroy live infra)", n)
	}
}

// TestReapFailed_BatchBounded asserts one pass deprovisions at most reapBatchLimit
// rows; the remainder is left for the next tick.
func TestReapFailed_BatchBounded(t *testing.T) {
	d := &deprovTracker{}
	m, pool := newDBManagerWithDeprov(t, Config{}, d)
	ctx := context.Background()

	total := reapBatchLimit + 5
	for i := 0; i < total; i++ {
		seedItem(t, pool, "mongodb", "failed", "pool-batch", failedReapGrace+time.Hour)
	}

	m.reapFailed(ctx)

	if got := len(d.snapshot()); got != reapBatchLimit {
		t.Fatalf("one pass deprovisioned %d items; want reapBatchLimit (%d)", got, reapBatchLimit)
	}
}

// TestReapFailed_QueryError — a SELECT error hits the query-error arm and
// returns without panicking or deprovisioning anything. Uses the fakeDB seam
// (DB-independent).
func TestReapFailed_QueryError(t *testing.T) {
	d := &deprovTracker{}
	db := &fakeDB{queryErr: errors.New("query boom")}
	m := newFakeDBManager(db, d)

	m.reapFailed(context.Background()) // must not panic
	if n := len(d.snapshot()); n != 0 {
		t.Fatalf("query error still deprovisioned %d items; want 0", n)
	}
}

// TestReportStuckAssigned_QueryError — a Query error hits the query-error arm
// and returns without panicking. Uses the fakeDB seam (DB-independent).
func TestReportStuckAssigned_QueryError(t *testing.T) {
	db := &fakeDB{queryErr: errors.New("query boom")}
	m := newFakeDBManager(db, &deprovTracker{})
	m.reportStuckAssigned(context.Background()) // must not panic
}

// TestReportStuckAssigned_ResetsToZero — a type that had stuck rows, then has
// none, must report zero (the gauge is reset per pass, not a high-water mark).
func TestReportStuckAssigned_ResetsToZero(t *testing.T) {
	d := &deprovTracker{}
	m, pool := newDBManagerWithDeprov(t, Config{PostgresSize: 1, RedisSize: 1}, d)
	ctx := context.Background()

	// First pass: one stuck postgres assigned row → gauge should read 1.
	id := seedItem(t, pool, "postgres", "assigned", "pool-stuck", stuckAssignedGrace+time.Hour)
	m.reportStuckAssigned(ctx)
	if got := testutil.ToFloat64(poolStuckAssignedGauge.WithLabelValues("postgres")); got != 1 {
		t.Fatalf("stuck-assigned gauge = %v after one stuck row; want 1", got)
	}

	// Remove it, run again: the gauge must drop back to 0 for postgres (reset
	// per pass, not a high-water mark).
	if _, err := pool.Exec(ctx, `DELETE FROM pool_items WHERE id = $1`, id); err != nil {
		t.Fatalf("delete stuck row: %v", err)
	}
	m.reportStuckAssigned(ctx)
	if got := testutil.ToFloat64(poolStuckAssignedGauge.WithLabelValues("postgres")); got != 0 {
		t.Fatalf("stuck-assigned gauge = %v after clearing stuck rows; want 0 (gauge not reset)", got)
	}
}

// --- hermetic fake pgxDB: drives the post-Query error arms ---
//
// A real Postgres connection never deterministically fails a Scan, Rows.Err, or
// the DELETE Exec on a row a SELECT just returned, so these defensive arms in
// reapFailed / reportStuckAssigned are only reachable via an injected fake (the
// "test seam not waiver" rule). fakeDB satisfies the package-private pgxDB
// interface; the Manager's db field accepts it directly.

// fakeRows implements pgx.Rows. It yields `rowCount` rows; Scan returns scanErr
// (on the call indicated by scanErrOn, 1-based) and Err returns rowsErr.
type fakeRows struct {
	rowCount  int
	delivered int
	scanErrOn int   // which Scan call (1-based) returns scanErr; 0 = never
	scanErr   error // returned by Scan on the scanErrOn-th call
	rowsErr   error // returned by Err()
}

func (r *fakeRows) Next() bool {
	if r.delivered >= r.rowCount {
		return false
	}
	r.delivered++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErrOn != 0 && r.delivered == r.scanErrOn {
		return r.scanErr
	}
	// Populate string destinations with a placeholder so a non-error Scan in
	// reportStuckAssigned (resource_type, count) does not panic. count is an int
	// destination there; tolerate both.
	for _, d := range dest {
		switch v := d.(type) {
		case *string:
			*v = "postgres"
		case *int:
			*v = 0
		}
	}
	return nil
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return r.rowsErr }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

// fakeDB returns a configurable fakeRows from Query and a configurable error
// from Exec (used for the DELETE arm). QueryRow is unused by the reaper.
type fakeDB struct {
	rows     *fakeRows
	queryErr error
	execErr  error
}

func (d *fakeDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	if d.queryErr != nil {
		return nil, d.queryErr
	}
	return d.rows, nil
}
func (d *fakeDB) QueryRow(context.Context, string, ...any) pgx.Row { return nil }
func (d *fakeDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, d.execErr
}

func newFakeDBManager(db pgxDB, d *deprovTracker) *Manager {
	return &Manager{
		db:        db,
		postgresB: &mockPostgresBackend{deprov: d},
		redisB:    &mockRedisBackend{deprov: d},
		mongoB:    &mockMongoBackend{deprov: d},
		queueB:    &mockQueueBackend{deprov: d},
		targets:   map[string]int{"postgres": 1},
	}
}

// TestReapFailed_ScanError — a Scan failure mid-iteration aborts the pass
// without deprovisioning anything (it cannot trust the partial row).
func TestReapFailed_ScanError(t *testing.T) {
	d := &deprovTracker{}
	db := &fakeDB{rows: &fakeRows{rowCount: 1, scanErrOn: 1, scanErr: errors.New("scan boom")}}
	m := newFakeDBManager(db, d)
	m.reapFailed(context.Background()) // must not panic
	if n := len(d.snapshot()); n != 0 {
		t.Fatalf("scan error still deprovisioned %d items; want 0", n)
	}
}

// TestReapFailed_RowsErr — a Rows.Err failure after iteration aborts the pass
// (the result set may be truncated) without deprovisioning.
func TestReapFailed_RowsErr(t *testing.T) {
	d := &deprovTracker{}
	db := &fakeDB{rows: &fakeRows{rowCount: 0, rowsErr: errors.New("rows boom")}}
	m := newFakeDBManager(db, d)
	m.reapFailed(context.Background())
	if n := len(d.snapshot()); n != 0 {
		t.Fatalf("rows.Err still deprovisioned %d items; want 0", n)
	}
}

// TestReapFailed_DeleteErr — Deprovision succeeds but the row DELETE fails: the
// delete_err arm is taken (counted) and the loop continues. We assert the
// Deprovision happened (so the infra was freed) and the call did not panic.
func TestReapFailed_DeleteErr(t *testing.T) {
	d := &deprovTracker{}
	db := &fakeDB{rows: &fakeRows{rowCount: 1}, execErr: errors.New("delete boom")}
	m := newFakeDBManager(db, d)
	m.reapFailed(context.Background())
	if n := len(d.snapshot()); n != 1 {
		t.Fatalf("expected 1 Deprovision before the failing DELETE, got %d", n)
	}
}

// TestReportStuckAssigned_ScanError — a Scan failure in the count loop aborts
// the pass without panicking.
func TestReportStuckAssigned_ScanError(t *testing.T) {
	db := &fakeDB{rows: &fakeRows{rowCount: 1, scanErrOn: 1, scanErr: errors.New("scan boom")}}
	m := newFakeDBManager(db, &deprovTracker{})
	m.reportStuckAssigned(context.Background())
}

// TestReportStuckAssigned_RowsErr — a Rows.Err failure aborts the pass.
func TestReportStuckAssigned_RowsErr(t *testing.T) {
	db := &fakeDB{rows: &fakeRows{rowCount: 0, rowsErr: errors.New("rows boom")}}
	m := newFakeDBManager(db, &deprovTracker{})
	m.reportStuckAssigned(context.Background())
}

// Compile-time assertions that the fakes satisfy the production interfaces.
var (
	_ pgxDB    = (*fakeDB)(nil)
	_ pgx.Rows = (*fakeRows)(nil)
)
