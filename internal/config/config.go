package config

import (
	"log/slog"
	"os"
	"strconv"
)

// Config holds all runtime configuration for the provisioner.
type Config struct {
	Port                     string // PROVISIONER_PORT, default "50051"
	PostgresProvisionBackend string // POSTGRES_PROVISION_BACKEND, default "local"
	PostgresCustomersURL     string // POSTGRES_CUSTOMERS_URL
	NeonAPIKey               string // NEON_API_KEY
	NeonRegionID             string // NEON_REGION_ID, default "aws-us-east-1"
	RedisProvisionBackend    string // REDIS_PROVISION_BACKEND, default "local"
	RedisProvisionHost       string // REDIS_PROVISION_HOST, default "localhost:6379"
	MongoProvisionBackend    string // MONGO_PROVISION_BACKEND, default "local"
	MongoAdminURI            string // MONGO_ADMIN_URI
	MongoHost                string // MONGO_HOST
	QueueProvisionBackend    string // QUEUE_PROVISION_BACKEND, default "local"
	ProvisionerSecret        string // PROVISIONER_SECRET — shared secret for auth interceptor

	// Dedicated provisioning — Team tier.
	// When set, the provisioner uses these for Team-tier ProvisionResource calls.
	DedicatedPostgresDSN string // DEDICATED_POSTGRES_DSN — admin DSN for dedicated Postgres cluster
	DedicatedRedisURL    string // DEDICATED_REDIS_URL — admin URL for dedicated Redis cluster
	UpstashAPIKey        string // UPSTASH_API_KEY — optional; enables Upstash path for dedicated Redis

	// PostgresClusterURLs is a comma-separated list of admin DSNs for shared Postgres
	// clusters. When set, the ClusterRouter distributes provisions across them by
	// capacity. When empty, PostgresCustomersURL is used as the single cluster.
	PostgresClusterURLs string // POSTGRES_CLUSTER_URLS

	// Pool configuration — hot-provisioning pool.
	ProvisionerDatabaseURL string // PROVISIONER_DATABASE_URL — provisioner's own Postgres
	AESKey                 string // AES_KEY (hex) — encrypts pool connection URLs at rest
	PoolPostgresSize       int    // POOL_POSTGRES_SIZE, default 10
	PoolRedisSize          int    // POOL_REDIS_SIZE, default 10
	PoolMongoSize          int    // POOL_MONGO_SIZE, default 10
	PoolQueueSize          int    // POOL_QUEUE_SIZE (NATS), default 10

	// ── Kubernetes dedicated backend (Team tier) ─────────────────────────────
	//
	// Set K8S_DEDICATED_BACKEND=true to provision dedicated k8s pods instead of
	// using Neon/Upstash or a shared admin DSN for Team-tier resources.
	//
	// Each provisioned token gets its own namespace (instant-customer-{token}),
	// with NetworkPolicy, ResourceQuota, PVC, Deployment, and NodePort Service.
	// Deprovision deletes the namespace (cascading GC of all resources).
	//
	// External access: the returned connection URL uses K8S_EXTERNAL_HOST + NodePort.
	//   - Local dev (Rancher Desktop): K8S_EXTERNAL_HOST=127.0.0.1
	//   - EKS (MVP): K8S_EXTERNAL_HOST={any-node-external-ip}
	//   - EKS (production): deploy a TCP proxy (Envoy/PgBouncer) behind one NLB,
	//     set K8S_EXTERNAL_HOST to the NLB DNS. See docs/ops-k8s-dedicated.md.
	K8sDedicatedBackend    bool   // K8S_DEDICATED_BACKEND — enable k8s backend for Team tier
	K8sKubeconfig          string // K8S_KUBECONFIG — path to kubeconfig; empty = in-cluster
	K8sExternalHost        string // K8S_EXTERNAL_HOST — hostname in returned connection URLs
	K8sStorageClass        string // K8S_STORAGE_CLASS — "gp3" (EKS) or "local-path" (dev); default "gp3"
	K8sPostgresImage       string // K8S_POSTGRES_IMAGE — default "postgres:16"
	K8sRedisImage          string // K8S_REDIS_IMAGE — default "redis:7-alpine"
	K8sMongoImage          string // K8S_MONGO_IMAGE — default "mongo:7"
	K8sPostgresStorageGi   int    // K8S_POSTGRES_STORAGE_GI — PVC size in GiB, default 50
	K8sRedisStorageGi      int    // K8S_REDIS_STORAGE_GI — PVC size in GiB, default 10
	K8sMongoStorageGi      int    // K8S_MONGO_STORAGE_GI — PVC size in GiB, default 50
	K8sNatsImage           string // K8S_NATS_IMAGE — default "nats:2.10-alpine"

	// NATSHost is the hostname (no port) of the shared NATS server.
	// Used by the local queue backend for shared-tier provisioning.
	NATSHost string // NATS_HOST, default "localhost"

	// MinIO — object storage usage queries (StorageBytes for 'storage' resources).
	// The provisioner never provisions storage resources (that's done by the API directly),
	// but it does query usage via ListObjects for the UpdateStorageBytes worker.
	MinioEndpoint     string // MINIO_ENDPOINT — host:port (e.g. minio.instant-data.svc.cluster.local:9000)
	MinioRootUser     string // MINIO_ROOT_USER — root access key
	MinioRootPassword string // MINIO_ROOT_PASSWORD — root secret key
	MinioBucketName   string // MINIO_BUCKET_NAME — shared bucket, default "instant-shared"
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// Load reads configuration from environment variables.
func Load() *Config {
	cfg := &Config{
		Port:                     getenv("PROVISIONER_PORT", "50051"),
		PostgresProvisionBackend: getenv("POSTGRES_PROVISION_BACKEND", "local"),
		PostgresCustomersURL:     getenv("POSTGRES_CUSTOMERS_URL", ""),
		PostgresClusterURLs:      os.Getenv("POSTGRES_CLUSTER_URLS"),
		NeonAPIKey:               os.Getenv("NEON_API_KEY"),
		NeonRegionID:             getenv("NEON_REGION_ID", "aws-us-east-1"),
		RedisProvisionBackend:    getenv("REDIS_PROVISION_BACKEND", "local"),
		RedisProvisionHost:       getenv("REDIS_PROVISION_HOST", "localhost:6379"),
		MongoProvisionBackend:    getenv("MONGO_PROVISION_BACKEND", "local"),
		MongoAdminURI:            getenv("MONGO_ADMIN_URI", "mongodb://root:root@localhost:27017"),
		MongoHost:                getenv("MONGO_HOST", "localhost:27017"),
		QueueProvisionBackend:    getenv("QUEUE_PROVISION_BACKEND", "local"),
		ProvisionerSecret:        os.Getenv("PROVISIONER_SECRET"),
		DedicatedPostgresDSN:     os.Getenv("DEDICATED_POSTGRES_DSN"),
		DedicatedRedisURL:        os.Getenv("DEDICATED_REDIS_URL"),
		UpstashAPIKey:            os.Getenv("UPSTASH_API_KEY"),
		ProvisionerDatabaseURL:   os.Getenv("PROVISIONER_DATABASE_URL"),
		AESKey:                   os.Getenv("AES_KEY"),
		// Pool targets default to 10 (was 2/3/2/2). A drained pool now refills
		// concurrently (see pool.maxRefillConcurrency), so a deeper warm buffer
		// no longer costs a proportionally longer serial refill — and 10 ready
		// items absorbs the load test's concurrency-8 burst entirely on the
		// fast pool-claim path instead of falling through to slow live
		// provisioning. Override per-type via the POOL_*_SIZE env vars.
		PoolPostgresSize:         getenvInt("POOL_POSTGRES_SIZE", 10),
		PoolRedisSize:            getenvInt("POOL_REDIS_SIZE", 10),
		PoolMongoSize:            getenvInt("POOL_MONGO_SIZE", 10),
		PoolQueueSize:            getenvInt("POOL_QUEUE_SIZE", 10),
		K8sDedicatedBackend:      os.Getenv("K8S_DEDICATED_BACKEND") == "true",
		K8sKubeconfig:            os.Getenv("K8S_KUBECONFIG"),
		K8sExternalHost:          os.Getenv("K8S_EXTERNAL_HOST"),
		K8sStorageClass:          getenv("K8S_STORAGE_CLASS", "gp3"),
		K8sPostgresImage:         getenv("K8S_POSTGRES_IMAGE", "postgres:16"),
		K8sRedisImage:            getenv("K8S_REDIS_IMAGE", "redis:7-alpine"),
		K8sMongoImage:            getenv("K8S_MONGO_IMAGE", "mongo:7"),
		K8sNatsImage:             getenv("K8S_NATS_IMAGE", "nats:2.10-alpine"),
		K8sPostgresStorageGi:     getenvInt("K8S_POSTGRES_STORAGE_GI", 50),
		K8sRedisStorageGi:        getenvInt("K8S_REDIS_STORAGE_GI", 10),
		K8sMongoStorageGi:        getenvInt("K8S_MONGO_STORAGE_GI", 50),
		NATSHost:                 getenv("NATS_HOST", "localhost"),
		MinioEndpoint:            os.Getenv("MINIO_ENDPOINT"),
		MinioRootUser:            os.Getenv("MINIO_ROOT_USER"),
		MinioRootPassword:        os.Getenv("MINIO_ROOT_PASSWORD"),
		MinioBucketName:          getenv("MINIO_BUCKET_NAME", "instant-shared"),
	}

	logStartupConfig(cfg)
	return cfg
}

func logStartupConfig(cfg *Config) {
	slog.Info("config.loaded",
		"port", cfg.Port,
		"postgres_provision_backend", cfg.PostgresProvisionBackend,
		"postgres_customers_url_set", cfg.PostgresCustomersURL != "",
		"neon_region_id", cfg.NeonRegionID,
		"neon_api_key_set", cfg.NeonAPIKey != "",
		"redis_provision_backend", cfg.RedisProvisionBackend,
		"redis_provision_host", cfg.RedisProvisionHost,
		"mongo_admin_uri_set", cfg.MongoAdminURI != "",
		"mongo_host", cfg.MongoHost,
		"queue_provision_backend", cfg.QueueProvisionBackend,
		"provisioner_secret_set", cfg.ProvisionerSecret != "",
		"dedicated_postgres_dsn_set", cfg.DedicatedPostgresDSN != "",
		"dedicated_redis_url_set", cfg.DedicatedRedisURL != "",
		"upstash_api_key_set", cfg.UpstashAPIKey != "",
		"provisioner_database_url_set", cfg.ProvisionerDatabaseURL != "",
		"aes_key_set", cfg.AESKey != "",
		"pool_postgres_size", cfg.PoolPostgresSize,
		"pool_redis_size", cfg.PoolRedisSize,
		"pool_mongo_size", cfg.PoolMongoSize,
		"k8s_dedicated_backend", cfg.K8sDedicatedBackend,
		"k8s_external_host_set", cfg.K8sExternalHost != "",
		"k8s_storage_class", cfg.K8sStorageClass,
		"minio_endpoint_set", cfg.MinioEndpoint != "",
		"minio_bucket_name", cfg.MinioBucketName,
	)
}
