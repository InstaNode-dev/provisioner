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
	"github.com/jackc/pgx/v5/pgxpool"

	"instant.dev/common/crypto"
	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/redis"
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
}

// Config holds pool sizing parameters per resource type. Zero disables that
// resource's pool — handed-out requests fall through to live provisioning.
type Config struct {
	PostgresSize int // target number of ready postgres items in pool
	RedisSize    int // target number of ready redis items in pool
	MongoSize    int // target number of ready mongodb items in pool
	QueueSize    int // target number of ready NATS items in pool
}

// Manager maintains a pool of pre-provisioned resources.
type Manager struct {
	db        *pgxpool.Pool
	aesKey    []byte
	postgresB postgres.Backend
	redisB    redis.Backend
	mongoB    mongo.Backend
	queueB    queue.Backend
	targets   map[string]int

	refillCh chan string
	done     chan struct{}
	wg       sync.WaitGroup
}

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

// Start initialises the schema, triggers an initial fill, and begins the maintenance loop.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.migrate(ctx); err != nil {
		return fmt.Errorf("pool.Start: migrate: %w", err)
	}

	m.wg.Add(1)
	go m.run(ctx)

	// Trigger initial refill for all resource types.
	for rt := range m.targets {
		m.triggerRefill(rt)
	}
	return nil
}

// Shutdown stops background goroutines.
func (m *Manager) Shutdown() {
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
		RETURNING id, connection_url, provider_resource_id, database_name, username, key_prefix
	`, resourceType)

	var (
		item   Item
		encURL string
	)
	err := row.Scan(&item.ID, &encURL, &item.ProviderResourceID,
		&item.DatabaseName, &item.Username, &item.KeyPrefix)
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
	ticker := time.NewTicker(30 * time.Second)
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

	for i := 0; i < needed; i++ {
		if err := m.provisionOneItem(ctx, resourceType); err != nil {
			slog.Error("pool.fillPool: provision", "resource_type", resourceType, "error", err)
			// Continue — provision as many items as possible.
		}
	}
}

func (m *Manager) provisionOneItem(ctx context.Context, resourceType string) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

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
			return fmt.Errorf("provision postgres: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return fmt.Errorf("encrypt postgres url: %w", err)
		}
		encURL = enc
		providerResourceID = creds.ProviderResourceID
		databaseName = creds.DatabaseName
		username = creds.Username

	case "redis":
		creds, err := m.redisB.Provision(ctx, poolToken, "anonymous")
		if err != nil {
			return fmt.Errorf("provision redis: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return fmt.Errorf("encrypt redis url: %w", err)
		}
		encURL = enc
		keyPrefix = creds.KeyPrefix

	case "mongodb":
		creds, err := m.mongoB.Provision(ctx, poolToken, "anonymous")
		if err != nil {
			return fmt.Errorf("provision mongodb: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return fmt.Errorf("encrypt mongodb url: %w", err)
		}
		encURL = enc
		databaseName = creds.DatabaseName

	case "queue":
		// NATS: same pattern as the data services. queue.Credentials has just
		// URL + ProviderResourceID; no per-resource database/user concept.
		creds, err := m.queueB.Provision(ctx, poolToken, "anonymous")
		if err != nil {
			return fmt.Errorf("provision queue: %w", err)
		}
		enc, err := crypto.Encrypt(m.aesKey, creds.URL)
		if err != nil {
			return fmt.Errorf("encrypt queue url: %w", err)
		}
		encURL = enc
		providerResourceID = creds.ProviderResourceID

	default:
		return fmt.Errorf("unknown resource type: %s", resourceType)
	}

	if _, err := m.db.Exec(ctx, `
		INSERT INTO pool_items
			(resource_type, connection_url, provider_resource_id, database_name, username, key_prefix)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, resourceType, encURL, providerResourceID, databaseName, username, keyPrefix); err != nil {
		return fmt.Errorf("insert pool item: %w", err)
	}

	slog.Info("pool.provisionOneItem: added", "resource_type", resourceType, "pool_token", poolToken)
	return nil
}

func (m *Manager) migrate(ctx context.Context) error {
	_, err := m.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pool_items (
			id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			resource_type        TEXT        NOT NULL,
			connection_url       TEXT        NOT NULL,
			provider_resource_id TEXT        NOT NULL DEFAULT '',
			database_name        TEXT        NOT NULL DEFAULT '',
			username             TEXT        NOT NULL DEFAULT '',
			key_prefix           TEXT        NOT NULL DEFAULT '',
			status               TEXT        NOT NULL DEFAULT 'ready',
			created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
			assigned_at          TIMESTAMPTZ
		);
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
