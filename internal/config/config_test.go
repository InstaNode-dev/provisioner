package config

// config_test.go — exhaustive coverage for the provisioner's env-driven Config
// loader. Every exported func, every getenv/getenvInt branch, and both the
// default and override arm of every field in Load() are exercised here.
//
// All env permutations use t.Setenv so the process environment is restored
// after each subtest — no global state leaks between cases.

import (
	"testing"
)

// allConfigEnvKeys is every environment variable Load() consults. Tests that
// want a clean baseline clear all of them first via t.Setenv("", ...) so a
// developer's ambient shell env (e.g. REDIS_URL) can't perturb assertions.
var allConfigEnvKeys = []string{
	"PROVISIONER_PORT",
	"POSTGRES_PROVISION_BACKEND",
	"POSTGRES_CUSTOMERS_URL",
	"POSTGRES_CLUSTER_URLS",
	"NEON_API_KEY",
	"NEON_REGION_ID",
	"REDIS_PROVISION_BACKEND",
	"REDIS_PROVISION_HOST",
	"MONGO_PROVISION_BACKEND",
	"MONGO_ADMIN_URI",
	"MONGO_HOST",
	"QUEUE_PROVISION_BACKEND",
	"PROVISIONER_SECRET",
	"DEDICATED_POSTGRES_DSN",
	"DEDICATED_REDIS_URL",
	"UPSTASH_API_KEY",
	"PROVISIONER_DATABASE_URL",
	"AES_KEY",
	"POOL_POSTGRES_SIZE",
	"POOL_REDIS_SIZE",
	"POOL_MONGO_SIZE",
	"POOL_QUEUE_SIZE",
	"K8S_DEDICATED_BACKEND",
	"K8S_KUBECONFIG",
	"K8S_EXTERNAL_HOST",
	"K8S_STORAGE_CLASS",
	"K8S_POSTGRES_IMAGE",
	"K8S_REDIS_IMAGE",
	"K8S_MONGO_IMAGE",
	"K8S_NATS_IMAGE",
	"K8S_POSTGRES_STORAGE_GI",
	"K8S_REDIS_STORAGE_GI",
	"K8S_MONGO_STORAGE_GI",
	"NATS_HOST",
	"MINIO_ENDPOINT",
	"MINIO_ROOT_USER",
	"MINIO_ROOT_PASSWORD",
	"MINIO_BUCKET_NAME",
}

// clearEnv unsets every config env var for the duration of the test so the
// default arm of every field is what's actually exercised.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allConfigEnvKeys {
		t.Setenv(k, "")
	}
}

// ── getenv ───────────────────────────────────────────────────────────────────

func TestGetenv(t *testing.T) {
	t.Run("returns value when set", func(t *testing.T) {
		t.Setenv("CFG_TEST_GETENV", "real")
		if got := getenv("CFG_TEST_GETENV", "fallback"); got != "real" {
			t.Errorf("getenv set = %q; want %q", got, "real")
		}
	})
	t.Run("returns fallback when empty", func(t *testing.T) {
		t.Setenv("CFG_TEST_GETENV", "")
		if got := getenv("CFG_TEST_GETENV", "fallback"); got != "fallback" {
			t.Errorf("getenv empty = %q; want %q", got, "fallback")
		}
	})
	t.Run("returns fallback when unset", func(t *testing.T) {
		// CFG_TEST_GETENV_UNSET is never set anywhere.
		if got := getenv("CFG_TEST_GETENV_UNSET", "fb"); got != "fb" {
			t.Errorf("getenv unset = %q; want %q", got, "fb")
		}
	})
}

// ── getenvInt ──────────────────────────────────────────────────────────────

func TestGetenvInt(t *testing.T) {
	t.Run("parses valid integer", func(t *testing.T) {
		t.Setenv("CFG_TEST_INT", "42")
		if got := getenvInt("CFG_TEST_INT", 7); got != 42 {
			t.Errorf("getenvInt valid = %d; want 42", got)
		}
	})
	t.Run("fallback on empty", func(t *testing.T) {
		t.Setenv("CFG_TEST_INT", "")
		if got := getenvInt("CFG_TEST_INT", 7); got != 7 {
			t.Errorf("getenvInt empty = %d; want 7", got)
		}
	})
	t.Run("fallback on non-numeric", func(t *testing.T) {
		// strconv.Atoi fails → fallback. Covers the err != nil arm.
		t.Setenv("CFG_TEST_INT", "not-a-number")
		if got := getenvInt("CFG_TEST_INT", 7); got != 7 {
			t.Errorf("getenvInt non-numeric = %d; want 7 (fallback)", got)
		}
	})
	t.Run("fallback on unset", func(t *testing.T) {
		if got := getenvInt("CFG_TEST_INT_UNSET", 99); got != 99 {
			t.Errorf("getenvInt unset = %d; want 99", got)
		}
	})
	t.Run("negative integer parses", func(t *testing.T) {
		t.Setenv("CFG_TEST_INT", "-1")
		if got := getenvInt("CFG_TEST_INT", 7); got != -1 {
			t.Errorf("getenvInt negative = %d; want -1", got)
		}
	})
}

// ── Load: default arm ────────────────────────────────────────────────────────

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	cfg := Load()

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"Port", cfg.Port, "50051"},
		{"PostgresProvisionBackend", cfg.PostgresProvisionBackend, "local"},
		{"PostgresCustomersURL", cfg.PostgresCustomersURL, ""},
		{"PostgresClusterURLs", cfg.PostgresClusterURLs, ""},
		{"NeonAPIKey", cfg.NeonAPIKey, ""},
		{"NeonRegionID", cfg.NeonRegionID, "aws-us-east-1"},
		{"RedisProvisionBackend", cfg.RedisProvisionBackend, "local"},
		{"RedisProvisionHost", cfg.RedisProvisionHost, "localhost:6379"},
		{"MongoProvisionBackend", cfg.MongoProvisionBackend, "local"},
		{"MongoAdminURI", cfg.MongoAdminURI, "mongodb://root:root@localhost:27017"},
		{"MongoHost", cfg.MongoHost, "localhost:27017"},
		{"QueueProvisionBackend", cfg.QueueProvisionBackend, "local"},
		{"ProvisionerSecret", cfg.ProvisionerSecret, ""},
		{"DedicatedPostgresDSN", cfg.DedicatedPostgresDSN, ""},
		{"DedicatedRedisURL", cfg.DedicatedRedisURL, ""},
		{"UpstashAPIKey", cfg.UpstashAPIKey, ""},
		{"ProvisionerDatabaseURL", cfg.ProvisionerDatabaseURL, ""},
		{"AESKey", cfg.AESKey, ""},
		{"K8sKubeconfig", cfg.K8sKubeconfig, ""},
		{"K8sExternalHost", cfg.K8sExternalHost, ""},
		{"K8sStorageClass", cfg.K8sStorageClass, "gp3"},
		{"K8sPostgresImage", cfg.K8sPostgresImage, "postgres:16"},
		{"K8sRedisImage", cfg.K8sRedisImage, "redis:7-alpine"},
		{"K8sMongoImage", cfg.K8sMongoImage, "mongo:7"},
		{"K8sNatsImage", cfg.K8sNatsImage, "nats:2.10-alpine"},
		{"NATSHost", cfg.NATSHost, "localhost"},
		{"MinioEndpoint", cfg.MinioEndpoint, ""},
		{"MinioRootUser", cfg.MinioRootUser, ""},
		{"MinioRootPassword", cfg.MinioRootPassword, ""},
		{"MinioBucketName", cfg.MinioBucketName, "instant-shared"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q; want %q", c.name, c.got, c.want)
		}
	}

	intChecks := []struct {
		name string
		got  int
		want int
	}{
		{"PoolPostgresSize", cfg.PoolPostgresSize, 10},
		{"PoolRedisSize", cfg.PoolRedisSize, 10},
		{"PoolMongoSize", cfg.PoolMongoSize, 10},
		{"PoolQueueSize", cfg.PoolQueueSize, 10},
		{"K8sPostgresStorageGi", cfg.K8sPostgresStorageGi, 50},
		{"K8sRedisStorageGi", cfg.K8sRedisStorageGi, 10},
		{"K8sMongoStorageGi", cfg.K8sMongoStorageGi, 50},
	}
	for _, c := range intChecks {
		if c.got != c.want {
			t.Errorf("%s = %d; want %d", c.name, c.got, c.want)
		}
	}

	if cfg.K8sDedicatedBackend {
		t.Errorf("K8sDedicatedBackend default = true; want false")
	}
}

// ── Load: override arm ───────────────────────────────────────────────────────

func TestLoad_Overrides(t *testing.T) {
	clearEnv(t)

	t.Setenv("PROVISIONER_PORT", "60000")
	t.Setenv("POSTGRES_PROVISION_BACKEND", "neon")
	t.Setenv("POSTGRES_CUSTOMERS_URL", "postgres://cust")
	t.Setenv("POSTGRES_CLUSTER_URLS", "postgres://c1,postgres://c2")
	t.Setenv("NEON_API_KEY", "neon-key")
	t.Setenv("NEON_REGION_ID", "aws-eu-west-1")
	t.Setenv("REDIS_PROVISION_BACKEND", "upstash")
	t.Setenv("REDIS_PROVISION_HOST", "redis.example:6380")
	t.Setenv("MONGO_PROVISION_BACKEND", "k8s")
	t.Setenv("MONGO_ADMIN_URI", "mongodb://admin:pw@m:27017")
	t.Setenv("MONGO_HOST", "m.example:27017")
	t.Setenv("QUEUE_PROVISION_BACKEND", "k8s")
	t.Setenv("PROVISIONER_SECRET", "shh")
	t.Setenv("DEDICATED_POSTGRES_DSN", "postgres://ded")
	t.Setenv("DEDICATED_REDIS_URL", "redis://ded")
	t.Setenv("UPSTASH_API_KEY", "up-key")
	t.Setenv("PROVISIONER_DATABASE_URL", "postgres://prov")
	t.Setenv("AES_KEY", "deadbeef")
	t.Setenv("POOL_POSTGRES_SIZE", "1")
	t.Setenv("POOL_REDIS_SIZE", "2")
	t.Setenv("POOL_MONGO_SIZE", "3")
	t.Setenv("POOL_QUEUE_SIZE", "4")
	t.Setenv("K8S_DEDICATED_BACKEND", "true")
	t.Setenv("K8S_KUBECONFIG", "/kube/config")
	t.Setenv("K8S_EXTERNAL_HOST", "1.2.3.4")
	t.Setenv("K8S_STORAGE_CLASS", "local-path")
	t.Setenv("K8S_POSTGRES_IMAGE", "postgres:15")
	t.Setenv("K8S_REDIS_IMAGE", "redis:6")
	t.Setenv("K8S_MONGO_IMAGE", "mongo:6")
	t.Setenv("K8S_NATS_IMAGE", "nats:2.9")
	t.Setenv("K8S_POSTGRES_STORAGE_GI", "100")
	t.Setenv("K8S_REDIS_STORAGE_GI", "20")
	t.Setenv("K8S_MONGO_STORAGE_GI", "200")
	t.Setenv("NATS_HOST", "nats.example")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ROOT_USER", "minioadmin")
	t.Setenv("MINIO_ROOT_PASSWORD", "miniopass")
	t.Setenv("MINIO_BUCKET_NAME", "my-bucket")

	cfg := Load()

	strChecks := []struct {
		name string
		got  string
		want string
	}{
		{"Port", cfg.Port, "60000"},
		{"PostgresProvisionBackend", cfg.PostgresProvisionBackend, "neon"},
		{"PostgresCustomersURL", cfg.PostgresCustomersURL, "postgres://cust"},
		{"PostgresClusterURLs", cfg.PostgresClusterURLs, "postgres://c1,postgres://c2"},
		{"NeonAPIKey", cfg.NeonAPIKey, "neon-key"},
		{"NeonRegionID", cfg.NeonRegionID, "aws-eu-west-1"},
		{"RedisProvisionBackend", cfg.RedisProvisionBackend, "upstash"},
		{"RedisProvisionHost", cfg.RedisProvisionHost, "redis.example:6380"},
		{"MongoProvisionBackend", cfg.MongoProvisionBackend, "k8s"},
		{"MongoAdminURI", cfg.MongoAdminURI, "mongodb://admin:pw@m:27017"},
		{"MongoHost", cfg.MongoHost, "m.example:27017"},
		{"QueueProvisionBackend", cfg.QueueProvisionBackend, "k8s"},
		{"ProvisionerSecret", cfg.ProvisionerSecret, "shh"},
		{"DedicatedPostgresDSN", cfg.DedicatedPostgresDSN, "postgres://ded"},
		{"DedicatedRedisURL", cfg.DedicatedRedisURL, "redis://ded"},
		{"UpstashAPIKey", cfg.UpstashAPIKey, "up-key"},
		{"ProvisionerDatabaseURL", cfg.ProvisionerDatabaseURL, "postgres://prov"},
		{"AESKey", cfg.AESKey, "deadbeef"},
		{"K8sKubeconfig", cfg.K8sKubeconfig, "/kube/config"},
		{"K8sExternalHost", cfg.K8sExternalHost, "1.2.3.4"},
		{"K8sStorageClass", cfg.K8sStorageClass, "local-path"},
		{"K8sPostgresImage", cfg.K8sPostgresImage, "postgres:15"},
		{"K8sRedisImage", cfg.K8sRedisImage, "redis:6"},
		{"K8sMongoImage", cfg.K8sMongoImage, "mongo:6"},
		{"K8sNatsImage", cfg.K8sNatsImage, "nats:2.9"},
		{"NATSHost", cfg.NATSHost, "nats.example"},
		{"MinioEndpoint", cfg.MinioEndpoint, "minio:9000"},
		{"MinioRootUser", cfg.MinioRootUser, "minioadmin"},
		{"MinioRootPassword", cfg.MinioRootPassword, "miniopass"},
		{"MinioBucketName", cfg.MinioBucketName, "my-bucket"},
	}
	for _, c := range strChecks {
		if c.got != c.want {
			t.Errorf("%s = %q; want %q", c.name, c.got, c.want)
		}
	}

	intChecks := []struct {
		name string
		got  int
		want int
	}{
		{"PoolPostgresSize", cfg.PoolPostgresSize, 1},
		{"PoolRedisSize", cfg.PoolRedisSize, 2},
		{"PoolMongoSize", cfg.PoolMongoSize, 3},
		{"PoolQueueSize", cfg.PoolQueueSize, 4},
		{"K8sPostgresStorageGi", cfg.K8sPostgresStorageGi, 100},
		{"K8sRedisStorageGi", cfg.K8sRedisStorageGi, 20},
		{"K8sMongoStorageGi", cfg.K8sMongoStorageGi, 200},
	}
	for _, c := range intChecks {
		if c.got != c.want {
			t.Errorf("%s = %d; want %d", c.name, c.got, c.want)
		}
	}

	if !cfg.K8sDedicatedBackend {
		t.Errorf("K8sDedicatedBackend with =true; want true")
	}
}

// TestLoad_K8sDedicatedBackend_NonTrueIsFalse asserts only the exact string
// "true" enables the boolean — any other truthy-looking value is false.
func TestLoad_K8sDedicatedBackend_NonTrueIsFalse(t *testing.T) {
	clearEnv(t)
	for _, v := range []string{"1", "TRUE", "yes", "false", "True"} {
		t.Setenv("K8S_DEDICATED_BACKEND", v)
		if Load().K8sDedicatedBackend {
			t.Errorf("K8S_DEDICATED_BACKEND=%q → true; want false (only exact \"true\")", v)
		}
	}
}

// TestLogStartupConfig_DoesNotPanic exercises logStartupConfig directly with a
// populated Config so every slog field reference is covered. Load() already
// calls it on the default + override configs above, but this guards the
// "secret_set"=true arms (non-empty secret fields) explicitly.
func TestLogStartupConfig_DoesNotPanic(t *testing.T) {
	cfg := &Config{
		Port:                   "50051",
		PostgresCustomersURL:   "set",
		NeonAPIKey:             "set",
		MongoAdminURI:          "set",
		ProvisionerSecret:      "set",
		DedicatedPostgresDSN:   "set",
		DedicatedRedisURL:      "set",
		UpstashAPIKey:          "set",
		ProvisionerDatabaseURL: "set",
		AESKey:                 "set",
		K8sExternalHost:        "set",
		MinioEndpoint:          "set",
		MinioBucketName:        "b",
	}
	// Must not panic; nothing returned to assert on.
	logStartupConfig(cfg)
}
