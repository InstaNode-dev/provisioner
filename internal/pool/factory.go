package pool

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/config"
)

// NewWithConfig creates a Manager using backend instances built from cfg.
// The pool manager gets its own set of backend instances (separate from the server's)
// so that pool refill goroutines don't contend with request-path provisioning.
func NewWithConfig(db *pgxpool.Pool, aesKey []byte, cfg Config, appCfg *config.Config) *Manager {
	postgresB := postgres.NewBackend(
		appCfg.PostgresProvisionBackend,
		appCfg.PostgresCustomersURL,
		appCfg.PostgresClusterURLs,
		appCfg.NeonAPIKey,
		appCfg.NeonRegionID,
	)
	redisB := redis.NewBackend(
		appCfg.RedisProvisionBackend,
		appCfg.RedisProvisionURL,
		appCfg.RedisProvisionHost,
	)
	mongoB := mongo.NewBackend(
		appCfg.MongoProvisionBackend,
		appCfg.MongoAdminURI,
		appCfg.MongoHost,
	)
	queueB := queue.NewBackend(
		appCfg.QueueProvisionBackend,
		appCfg.NATSHost,
	)
	return New(db, aesKey, cfg, postgresB, redisB, mongoB, queueB)
}
