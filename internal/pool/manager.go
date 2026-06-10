// Package pool maintains a hot-provisioning pool of pre-provisioned resources.
// Resources are provisioned ahead of time and assigned on demand, reducing
// provision latency from ~50ms (live) to <5ms (pool hit).
//
// Pool sizes are driven by configuration and maintained by a background goroutine.
// If the pool is empty, ProvisionResource falls back to live provisioning with no
// degradation in correctness — only latency.
package pool

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"instant.dev/common/crypto"
	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/dropguard"
)

// Item is a pre-provisioned resource claimed from the pool.
type Item struct {
	ID                 string
	ResourceType       string
	ConnectionURL      string // plaintext
	ProviderResourceID string
	DatabaseName       string
	Username           string
	KeyPrefix          string

	// PoolToken is the synthetic "pool-<uuid>" token the backing infrastructure
	// was actually provisioned under. The shared backends derive every infra
	// name (db_/usr_/keyspace) from it, so it — not the claiming caller's real
	// token — is the canonical naming token for this resource's lifecycle.
	// See internal/poolident for how it is threaded onto provider_resource_id
	// and resolved by Deprovision / StorageBytes / Regrade (P0-2).
	PoolToken string
}

// Config holds pool sizing parameters per resource type. Zero disables that
// resource's pool — handed-out requests fall through to live provisioning.
type Config struct {
	PostgresSize int // target number of ready postgres items in pool
	RedisSize    int // target number of ready redis items in pool
	MongoSize    int // target number of ready mongodb items in pool
	QueueSize    int // target number of ready NATS items in pool
}

// maxRefillConcurrency bounds the number of backend provisions fillPool runs
// in parallel when topping up a drained pool.
//
// Before this constant existed, fillPool provisioned its `needed` items in a
// strictly sequential `for` loop on the single maintenance goroutine, so a
// pool drained by a concurrency burst refilled at ~1 item per single-provision
// latency (15-25s on shared backends). That made the pool useless as a burst
// absorber — the load test's F1 cliff — because under concurrency ≥ 8 the pool
// stayed empty for the whole run and every request paid full live-provision
// latency. Refilling `needed` items concurrently lets a drained pool recover
// in roughly one single-provision window instead of N of them.
//
// It is bounded (not unbounded) so a deep deficit cannot open hundreds of
// simultaneous admin connections against the shared customer Postgres/Redis/
// Mongo and starve the request-path provisions of connection slots. 8 matches
// the concurrency the load test exercises and stays well under any backend's
// connection ceiling.
const maxRefillConcurrency = 8

// pgxDB is the narrow slice of *pgxpool.Pool the Manager actually uses
// (Query / QueryRow / Exec). Declaring it as an interface lets a test inject a
// fake that drives the post-Query error arms — a Scan failure, a Rows.Err
// failure, a DELETE Exec failure — which a real Postgres connection never
// surfaces deterministically. *pgxpool.Pool satisfies this interface as-is, so
// production wiring (main.go) is unchanged; New still takes a *pgxpool.Pool.
type pgxDB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Manager maintains a pool of pre-provisioned resources.
type Manager struct {
	db        pgxDB
	aesKey    []byte
	postgresB postgres.Backend
	redisB    redis.Backend
	mongoB    mongo.Backend
	queueB    queue.Backend
	targets   map[string]int

	refillCh chan string
	done     chan struct{}
	wg       sync.WaitGroup

	// runCtx is the lifecycle context for all background work — the maintenance
	// loop and every fillPool / provisionOneItem it spawns derive from it.
	// Shutdown cancels runCancel so an in-flight provision aborts promptly
	// instead of running to its own 60s timeout against a process that is
	// already tearing down (BugBash 2026-05-18 P3 — "pool shutdown ctx").
	runCtx    context.Context
	runCancel context.CancelFunc

	// tickInterval is the period of the maintenance loop's health-check ticker.
	// Zero means the production default (30s); a test sets a tiny value to
	// exercise the periodic top-up arm without waiting 30 wall-clock seconds.
	tickInterval time.Duration
}

// defaultTickInterval is the maintenance loop's periodic health-check period.
const defaultTickInterval = 30 * time.Second

// Reaper grace windows (sweep #8).
//
// failedReapGrace is how long a 'failed' pool_item (one Discard marked
// unusable) must sit before the reaper deprovisions its backing infra and
// deletes the row. Discard sets assigned_at = now() at the moment it marks the
// item failed, so the grace measures time-since-discard. It is short because a
// 'failed' row has, by construction, no owning resources row — Discard is only
// called on the provisioner-side claim path BEFORE the item is ever returned
// to api, so nothing else can be mid-bind on it. The grace exists only to
// avoid racing a Discard that is still committing in a sibling request.
//
// stuckAssignedGrace is the age past which an 'assigned' row is reported on the
// instant_pool_stuck_assigned gauge. It is deliberately NOT a deprovision
// trigger: see reapStale and metrics.go for why the provisioner cannot safely
// deprovision an old 'assigned' item.
const (
	failedReapGrace    = 10 * time.Minute
	stuckAssignedGrace = 30 * time.Minute
)

// reapBatchLimit bounds how many failed rows one reap pass deprovisions, so a
// large backlog cannot monopolise the maintenance goroutine (and the bounded
// pgxpool) on a single tick. The remainder is picked up on the next tick.
const reapBatchLimit = 50

// New creates a Manager. Call Start to begin background maintenance.
func New(db *pgxpool.Pool, aesKey []byte, cfg Config,
	postgresB postgres.Backend, redisB redis.Backend, mongoB mongo.Backend, queueB queue.Backend,
) *Manager {
	return &Manager{
		db:        db,
		aesKey:    aesKey,
		postgresB: postgresB,
		redisB:    redisB,
		mongoB:    mongoB,
		queueB:    queueB,
		targets: map[string]int{
			"postgres": cfg.PostgresSize,
			"redis":    cfg.RedisSize,
			"mongodb":  cfg.MongoSize,
			"queue":    cfg.QueueSize,
		},
		refillCh: make(chan string, 40),
		done:     make(chan struct{}),
	}
}

// Start initialises the schema, triggers an initial fill, and begins the
// maintenance loop. The passed ctx bounds only the one-time migrate step; all
// recurring background work runs under an internal context (runCtx) that
// Shutdown cancels, so the caller can pass a request-scoped or background ctx
// without it dictating the maintenance loop's lifetime.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.migrate(ctx); err != nil {
		return fmt.Errorf("pool.Start: migrate: %w", err)
	}

	m.runCtx, m.runCancel = context.WithCancel(context.Background())

	m.wg.Add(1)
	go m.run(m.runCtx)

	// Trigger initial refill for all resource types.
	for rt := range m.targets {
		m.triggerRefill(rt)
	}
	return nil
}

// Shutdown stops background goroutines and aborts any in-flight pool fill.
// It cancels runCtx first so a provisionOneItem mid-flight returns promptly
// (its 60s timeout is derived from runCtx), then closes done to break the
// maintenance loop's select, then waits for run to exit. Calling Shutdown
// before Start (runCancel still nil) is a no-op on the cancel step.
func (m *Manager) Shutdown() {
	if m.runCancel != nil {
		m.runCancel()
	}
	close(m.done)
	m.wg.Wait()
}

// Claim atomically claims a ready pool item for the given resource type.
// Returns (nil, nil) if the pool is empty — caller should fall back to live provisioning.
func (m *Manager) Claim(ctx context.Context, resourceType string) (*Item, error) {
	row := m.db.QueryRow(ctx, `
		UPDATE pool_items
		SET    status = 'assigned', assigned_at = now()
		WHERE  id = (
			SELECT id FROM pool_items
			WHERE  resource_type = $1 AND status = 'ready'
			ORDER  BY created_at ASC
			LIMIT  1
			FOR    UPDATE SKIP LOCKED
		)
		RETURNING id, connection_url, provider_resource_id, database_name, username, key_prefix, pool_token
	`, resourceType)

	var (
		item   Item
		encURL string
	)
	err := row.Scan(&item.ID, &encURL, &item.ProviderResourceID,
		&item.DatabaseName, &item.Username, &item.KeyPrefix, &item.PoolToken)
	if err == pgx.ErrNoRows {
		return nil, nil // pool empty — caller falls back to live provisioning
	}
	if err != nil {
		return nil, fmt.Errorf("pool.Claim: scan: %w", err)
	}

	url, err := crypto.Decrypt(m.aesKey, encURL)
	if err != nil {
		return nil, fmt.Errorf("pool.Claim: decrypt: %w", err)
	}
	item.ResourceType = resourceType
	item.ConnectionURL = url

	slog.Info("pool.Claim: hit", "resource_type", resourceType, "pool_id", item.ID)

	// Trigger async refill to top up the pool.
	m.triggerRefill(resourceType)
	return &item, nil
}

// Discard marks a previously-Claimed item as 'failed' so it is never handed
// out again, and triggers a refill to replace it. Callers invoke this when
// they cannot safely use a claimed item (e.g. the connection-limit regrade
// failed, or the item carries no pool token) and fall back to live
// provisioning. Without it the row stays 'assigned' forever with no owning
// resource row — a slow leak of pre-provisioned backing infra that no sweeper
// can distinguish from a legitimately in-use item (bug bash 2026-06-02 #3).
func (m *Manager) Discard(ctx context.Context, item *Item) error {
	if item == nil {
		return nil
	}
	if _, err := m.db.Exec(ctx, `
		UPDATE pool_items
		SET    status = 'failed', assigned_at = now()
		WHERE  id = $1
	`, item.ID); err != nil {
		return fmt.Errorf("pool.Discard item %s: %w", item.ID, err)
	}
	slog.Warn("pool.Discard: claimed item marked failed (unusable, falling back to live)",
		"pool_id", item.ID, "resource_type", item.ResourceType)
	m.triggerRefill(item.ResourceType)
	return nil
}

// reapStale is the hot-pool reaper (sweep #8). On each maintenance tick it:
//
//  1. Deprovisions + deletes 'failed' pool_items older than failedReapGrace.
//     A 'failed' row is one Discard marked unusable on the provisioner-side
//     claim path BEFORE the item was ever returned to api, so its backing
//     infra (db_pool-<uuid> / usr_pool-<uuid> / keyspace pool-<uuid>:*) is
//     owned by NO resources row and would otherwise leak forever — the
//     worker's resource-TTL reaper never sees it. Deprovision is idempotent
//     (DROP ... IF EXISTS), so reaping an item whose infra is already gone is
//     a safe no-op.
//
//  2. Reports — but does NOT deprovision — 'assigned' pool_items older than
//     stuckAssignedGrace on the instant_pool_stuck_assigned gauge. From the
//     provisioner's own DB an orphaned (crashed-claim) 'assigned' row is
//     indistinguishable from one a live api request successfully bound to a
//     resources row: there is no write-back when the bind succeeds. The
//     backing infra of a BOUND item is owned by that resources row and reaped
//     by the worker's resource-TTL path; deprovisioning it here would destroy
//     live customer infra (the truehomie-db DROP incident class). A safe
//     orphan-'assigned' reaper needs an anti-join against the resources table,
//     which lives in a different database than pool_items — so it cannot be
//     done from the provisioner. The gauge gives the operator the signal; the
//     fix is tracked in the PR description.
//
// reapStale never returns an error — it is best-effort background maintenance
// and logs+counts every failure so a wedged reaper is observable rather than
// fatal.
func (m *Manager) reapStale(ctx context.Context) {
	m.reapFailed(ctx)
	m.reportStuckAssigned(ctx)
}

// reapFailed deprovisions the backing infra for, and deletes, 'failed'
// pool_items older than failedReapGrace. Bounded to reapBatchLimit rows per
// pass. Each row is deprovisioned through the resource-type's backend using
// its pool_token as the naming token (the stored provider_resource_id carries
// no pooltok marker, so NamingToken falls back to the token we pass — which is
// exactly the pool_token the infra was named from). A Deprovision failure
// leaves the row in place so the next tick retries; only a clean Deprovision
// deletes the row, so infra is never orphaned by deleting its tracking row
// first.
func (m *Manager) reapFailed(ctx context.Context) {
	rows, err := m.db.Query(ctx, `
		SELECT id, resource_type, provider_resource_id, pool_token
		FROM   pool_items
		WHERE  status = 'failed'
		AND    assigned_at IS NOT NULL
		AND    assigned_at < now() - $1::interval
		ORDER  BY assigned_at ASC
		LIMIT  $2
	`, failedReapGrace.String(), reapBatchLimit)
	if err != nil {
		slog.Error("pool.reapFailed: query", "error", err)
		return
	}

	type stale struct {
		id, resourceType, providerResourceID, poolToken string
	}
	var batch []stale
	for rows.Next() {
		var s stale
		if err := rows.Scan(&s.id, &s.resourceType, &s.providerResourceID, &s.poolToken); err != nil {
			slog.Error("pool.reapFailed: scan", "error", err)
			rows.Close()
			return
		}
		batch = append(batch, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		slog.Error("pool.reapFailed: rows", "error", err)
		return
	}

	for _, s := range batch {
		if err := m.deprovisionBacking(ctx, s.resourceType, s.poolToken, s.providerResourceID); err != nil {
			slog.Warn("pool.reapFailed: deprovision failed (leaving row for retry)",
				"pool_id", s.id, "resource_type", s.resourceType, "error", err)
			poolReapTotal.WithLabelValues(s.resourceType, "failed", "deprovision_err").Inc()
			continue
		}
		if _, err := m.db.Exec(ctx, `DELETE FROM pool_items WHERE id = $1`, s.id); err != nil {
			slog.Error("pool.reapFailed: delete row after deprovision (infra freed, row orphaned until next tick)",
				"pool_id", s.id, "error", err)
			poolReapTotal.WithLabelValues(s.resourceType, "failed", "delete_err").Inc()
			continue
		}
		slog.Info("pool.reapFailed: reaped leaked failed item",
			"pool_id", s.id, "resource_type", s.resourceType, "pool_token", s.poolToken)
		poolReapTotal.WithLabelValues(s.resourceType, "failed", "reaped").Inc()
	}
}

// reportStuckAssigned refreshes the instant_pool_stuck_assigned gauge with the
// per-type count of 'assigned' rows older than stuckAssignedGrace. It does NOT
// deprovision — see reapStale's doc for why the provisioner cannot safely reap
// 'assigned' items. The gauge is reset for every configured resource type each
// pass so a type that drops back to zero stuck rows reports zero (a Set-only
// gauge would otherwise hold a stale high-water mark).
func (m *Manager) reportStuckAssigned(ctx context.Context) {
	counts := make(map[string]int, len(m.targets))
	for rt := range m.targets {
		counts[rt] = 0
	}

	rows, err := m.db.Query(ctx, `
		SELECT resource_type, count(*)
		FROM   pool_items
		WHERE  status = 'assigned'
		AND    assigned_at IS NOT NULL
		AND    assigned_at < now() - $1::interval
		GROUP  BY resource_type
	`, stuckAssignedGrace.String())
	if err != nil {
		slog.Error("pool.reportStuckAssigned: query", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var rt string
		var n int
		if err := rows.Scan(&rt, &n); err != nil {
			slog.Error("pool.reportStuckAssigned: scan", "error", err)
			return
		}
		counts[rt] = n
	}
	if err := rows.Err(); err != nil {
		slog.Error("pool.reportStuckAssigned: rows", "error", err)
		return
	}

	for rt, n := range counts {
		poolStuckAssignedGauge.WithLabelValues(rt).Set(float64(n))
		if n > 0 {
			slog.Warn("pool.reportStuckAssigned: items stuck in 'assigned' past grace (operator signal — not auto-reaped)",
				"resource_type", rt, "count", n, "grace", stuckAssignedGrace.String())
		}
	}
}

// deprovisionBacking destroys the backing infra for one pool item via the
// resource-type's backend. token is the pool_token (the canonical naming token
// the infra was provisioned under); providerResourceID is the row's stored
// value (e.g. "local:0" for shared Postgres, "" for shared Redis/Mongo). The
// backends derive the real db_/usr_/keyspace names from the naming token, so
// passing the pool_token here targets exactly the infra the pool created.
func (m *Manager) deprovisionBacking(ctx context.Context, resourceType, token, providerResourceID string) error {
	// Name-convention guard + DDL-audit (truehomie hardening, task D3). The
	// pool reaper is the ONE customer-infra drop dispatch that does not pass
	// through server.guardedDrop (it has no gRPC request), so it carries its
	// own copy of the chokepoint contract: validate the naming token, then
	// emit the same `provisioner.drop` audit event BEFORE the backend executes
	// — the NR DDL-trap alert correlates shared-cluster DROP statements
	// against these events, and an un-logged drop path would page as
	// unsanctioned.
	if guardErr := dropguard.CheckNamingToken(token); guardErr != nil {
		slog.Error("provisioner.drop.refused",
			"event", "provisioner.drop.refused", "site", "pool.deprovisionBacking",
			"token", token, "provider_resource_id", providerResourceID,
			"resource_type", poolResourceTypeProto(resourceType), "error", guardErr)
		return fmt.Errorf("pool.deprovisionBacking: %w", guardErr)
	}
	slog.Info("provisioner.drop",
		"event", "provisioner.drop",
		"token", token,
		"provider_resource_id", providerResourceID,
		"resource_type", poolResourceTypeProto(resourceType),
		"backend", "shared",
		"request_id", "",
		"caller", "pool_reaper",
	)
	switch resourceType {
	case "postgres":
		return m.postgresB.Deprovision(ctx, token, providerResourceID)
	case "redis":
		return m.redisB.Deprovision(ctx, token, providerResourceID)
	case "mongodb":
		return m.mongoB.Deprovision(ctx, token, providerResourceID)
	case "queue":
		return m.queueB.Deprovision(ctx, token, providerResourceID)
	default:
		return fmt.Errorf("pool.deprovisionBacking: unknown resource type: %s", resourceType)
	}
}

// Stats returns the count of ready items per resource type.
func (m *Manager) Stats(ctx context.Context) (map[string]int, error) {
	rows, err := m.db.Query(ctx, `
		SELECT resource_type, count(*) AS cnt
		FROM   pool_items
		WHERE  status = 'ready'
		GROUP  BY resource_type
	`)
	if err != nil {
		return nil, fmt.Errorf("pool.Stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]int)
	for rows.Next() {
		var rt string
		var cnt int
		if err := rows.Scan(&rt, &cnt); err != nil {
			return nil, err
		}
		stats[rt] = cnt
	}
	return stats, rows.Err()
}

// --- internal ---

func (m *Manager) triggerRefill(resourceType string) {
	select {
	case m.refillCh <- resourceType:
	default:
		// Channel full — a refill is already queued for this type.
	}
}

func (m *Manager) run(ctx context.Context) {
	defer m.wg.Done()
	interval := m.tickInterval
	if interval <= 0 {
		interval = defaultTickInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case rt := <-m.refillCh:
			m.fillPool(ctx, rt)
		case <-ticker.C:
			// Periodic health check — fills any type that has dipped below target.
			for rt := range m.targets {
				m.fillPool(ctx, rt)
			}
			// Reap leaked 'failed' items + surface stuck 'assigned' ones
			// (sweep #8). Runs on the same cadence as the top-up so a leak is
			// cleaned within one tick interval of crossing its grace.
			m.reapStale(ctx)
		}
	}
}

func (m *Manager) fillPool(ctx context.Context, resourceType string) {
	target := m.targets[resourceType]
	if target <= 0 {
		return
	}

	var count int
	if err := m.db.QueryRow(ctx, `
		SELECT count(*) FROM pool_items WHERE resource_type = $1 AND status = 'ready'
	`, resourceType).Scan(&count); err != nil {
		slog.Error("pool.fillPool: count", "resource_type", resourceType, "error", err)
		return
	}

	needed := target - count
	if needed <= 0 {
		return
	}

	slog.Info("pool.fillPool: topping up", "resource_type", resourceType,
		"current", count, "target", target, "provisioning", needed)

	m.provisionItemsConcurrently(ctx, resourceType, needed)
}

// provisionItemsConcurrently provisions `needed` pool items in parallel, bounded
// to maxRefillConcurrency in-flight backend calls. Each item runs the full slow
// backend Provision; running them concurrently — rather than the old sequential
// `for` loop — lets a drained pool refill in roughly one single-provision window
// instead of N of them. This is the core of the F1 latency-cliff fix.
//
// Split out of fillPool (and exercised directly by the regression test) so the
// concurrency property can be asserted without a live DB: a test injects backends
// with an artificial delay and checks aggregate wall time is far below
// needed × single-latency.
func (m *Manager) provisionItemsConcurrently(ctx context.Context, resourceType string, needed int) {
	if needed <= 0 {
		return
	}
	// sem bounds concurrent backend provisions; cap the worker count at `needed`
	// so a small top-up does not spin up maxRefillConcurrency idle slots.
	limit := maxRefillConcurrency
	if needed < limit {
		limit = needed
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for i := 0; i < needed; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := m.provisionOneItem(ctx, resourceType); err != nil {
				slog.Error("pool.fillPool: provision", "resource_type", resourceType, "error", err)
				// Continue — provision as many items as possible.
			}
		}()
	}
	wg.Wait()
}

// provisionOneItem provisions a single pool item: it runs the slow backend
// Provision, then persists the resulting row. The two phases are split into
// provisionOneItemBackend (no DB) and the INSERT below so the backend phase is
// independently unit-testable. The whole operation is concurrency-safe — it
// shares no mutable Manager state with sibling calls — which is what makes
// provisionItemsConcurrently correct.
func (m *Manager) provisionOneItem(ctx context.Context, resourceType string) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	item, err := m.provisionOneItemBackend(ctx, resourceType)
	if err != nil {
		return err
	}

	if _, err := m.db.Exec(ctx, `
		INSERT INTO pool_items
			(resource_type, connection_url, provider_resource_id, database_name, username, key_prefix, pool_token)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, resourceType, item.encURL, item.providerResourceID, item.databaseName,
		item.username, item.keyPrefix, item.poolToken); err != nil {
		return fmt.Errorf("insert pool item: %w", err)
	}

	slog.Info("pool.provisionOneItem: added", "resource_type", resourceType, "pool_token", item.poolToken)
	return nil
}

// provisionedItem is the result of provisionOneItemBackend — the encrypted
// connection URL plus the identifiers needed to persist the pool_items row.
type provisionedItem struct {
	encURL             string
	providerResourceID string
	databaseName       string
	username           string
	keyPrefix          string
	poolToken          string
}

// provisionOneItemBackend runs the slow backend Provision for one pool item and
// returns the encrypted credentials. It touches NO Manager-shared mutable state
// (no m.db, no maps) so N concurrent calls are race-free — the property
// provisionItemsConcurrently relies on. Kept separate from the DB INSERT so a
// hermetic test can drive it with mock backends and no Postgres.
func (m *Manager) provisionOneItemBackend(ctx context.Context, resourceType string) (provisionedItem, error) {
	// Pool tokens use a distinct prefix so they're identifiable in backend logs.
	// Use "pool-" (hyphen), not "pool_" — k8s namespace names are RFC 1123,
	// which disallows underscores.
	poolToken := "pool-" + uuid.NewString()

	var (
		encURL             string
		providerResourceID string
		databaseName       string
		username           string
		keyPrefix          string
	)

	switch resourceType {
	case "postgres":
		// Pool items are always provisioned at anonymous tier (24h TTL, smallest
		// footprint). connLimit for anonymous is 2 (from plans.yaml); pass -1 to
		// let the backend use the tier default — the pool item will be re-graded
		// by the entitlement reconciler when it is assigned to a real team.
		creds, err := m.postgresB.Provision(ctx, poolToken, "anonymous", -1)
		if err != nil {
			return provisionedItem{}, fmt.Errorf("provision postgres: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return provisionedItem{}, fmt.Errorf("encrypt postgres url: %w", err)
		}
		encURL = enc
		providerResourceID = creds.ProviderResourceID
		databaseName = creds.DatabaseName
		username = creds.Username

	case "redis":
		creds, err := m.redisB.Provision(ctx, poolToken, "anonymous")
		if err != nil {
			return provisionedItem{}, fmt.Errorf("provision redis: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return provisionedItem{}, fmt.Errorf("encrypt redis url: %w", err)
		}
		encURL = enc
		keyPrefix = creds.KeyPrefix

	case "mongodb":
		creds, err := m.mongoB.Provision(ctx, poolToken, "anonymous")
		if err != nil {
			return provisionedItem{}, fmt.Errorf("provision mongodb: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return provisionedItem{}, fmt.Errorf("encrypt mongodb url: %w", err)
		}
		encURL = enc
		databaseName = creds.DatabaseName

	case "queue":
		// NATS: same pattern as the data services. queue.Credentials has just
		// URL + ProviderResourceID; no per-resource database/user concept.
		creds, err := m.queueB.Provision(ctx, poolToken, "anonymous")
		if err != nil {
			return provisionedItem{}, fmt.Errorf("provision queue: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return provisionedItem{}, fmt.Errorf("encrypt queue url: %w", err)
		}
		encURL = enc
		providerResourceID = creds.ProviderResourceID

	default:
		return provisionedItem{}, fmt.Errorf("unknown resource type: %s", resourceType)
	}

	return provisionedItem{
		encURL:             encURL,
		providerResourceID: providerResourceID,
		databaseName:       databaseName,
		username:           username,
		keyPrefix:          keyPrefix,
		poolToken:          poolToken,
	}, nil
}

func (m *Manager) migrate(ctx context.Context) error {
	// pool_token records the synthetic "pool-<uuid>" token the backing infra was
	// provisioned under. It is the canonical naming token for the resource's
	// whole lifecycle (P0-2); the ALTER below adds it to clusters created before
	// this column existed. Pre-existing rows backfill to '' — those pool items
	// were already provisioned and, once claimed, would still leak; the column
	// only guarantees correctness for items provisioned from now on. Stale
	// pre-fix ready items are best drained/recycled by an operator.
	_, err := m.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pool_items (
			id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			resource_type        TEXT        NOT NULL,
			connection_url       TEXT        NOT NULL,
			provider_resource_id TEXT        NOT NULL DEFAULT '',
			database_name        TEXT        NOT NULL DEFAULT '',
			username             TEXT        NOT NULL DEFAULT '',
			key_prefix           TEXT        NOT NULL DEFAULT '',
			pool_token           TEXT        NOT NULL DEFAULT '',
			status               TEXT        NOT NULL DEFAULT 'ready',
			created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
			assigned_at          TIMESTAMPTZ
		);
		ALTER TABLE pool_items ADD COLUMN IF NOT EXISTS pool_token TEXT NOT NULL DEFAULT '';
		CREATE INDEX IF NOT EXISTS idx_pool_ready
			ON pool_items (resource_type, created_at)
			WHERE status = 'ready';
	`)
	if err != nil {
		return fmt.Errorf("create pool_items: %w", err)
	}
	slog.Info("pool.migrate: schema ready")
	return nil
}

// Keep rand imported for the uuid package's internal use.
var _ = rand.Reader

// poolResourceTypeProto maps a pool_items.resource_type value to the proto
// enum string used by server.guardedDrop's audit events, so one NR query
// (resource_type = 'RESOURCE_TYPE_POSTGRES' …) covers RPC drops and pool-reap
// drops alike.
func poolResourceTypeProto(resourceType string) string {
	switch resourceType {
	case "postgres":
		return "RESOURCE_TYPE_POSTGRES"
	case "redis":
		return "RESOURCE_TYPE_REDIS"
	case "mongodb":
		return "RESOURCE_TYPE_MONGODB"
	case "queue":
		return "RESOURCE_TYPE_QUEUE"
	default:
		return "RESOURCE_TYPE_UNSPECIFIED"
	}
}
