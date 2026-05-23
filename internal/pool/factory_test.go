package pool

// factory_test.go — NewWithConfig wires backend instances from a *config.Config
// and hands them to New. It must construct a usable Manager without touching any
// real infrastructure: the backend constructors are lazy (they don't dial until
// a Provision call), so this is a pure wiring assertion.

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"instant.dev/provisioner/internal/config"
)

// TestNewWithConfig_WiresBackends — NewWithConfig must return a non-nil Manager
// with every backend slot populated and the targets map carrying the configured
// sizes. A nil db is fine here; we never call a DB-touching method.
func TestNewWithConfig_WiresBackends(t *testing.T) {
	appCfg := &config.Config{
		PostgresProvisionBackend: "local",
		PostgresCustomersURL:     "postgres://nobody@127.0.0.1:1/postgres?sslmode=disable",
		RedisProvisionBackend:    "local",
		RedisProvisionHost:       "127.0.0.1:6379",
		MongoProvisionBackend:    "local",
		MongoAdminURI:            "mongodb://root:root@127.0.0.1:27017",
		MongoHost:                "127.0.0.1:27017",
		QueueProvisionBackend:    "local",
		NATSHost:                 "127.0.0.1",
	}
	cfg := Config{PostgresSize: 3, RedisSize: 2, MongoSize: 1, QueueSize: 4}

	var db *pgxpool.Pool // nil — NewWithConfig must not touch it
	aesKey := make([]byte, 32)

	m := NewWithConfig(db, aesKey, cfg, appCfg)
	if m == nil {
		t.Fatal("NewWithConfig returned nil")
	}
	if m.postgresB == nil || m.redisB == nil || m.mongoB == nil || m.queueB == nil {
		t.Fatal("NewWithConfig left a backend slot nil")
	}
	wantTargets := map[string]int{"postgres": 3, "redis": 2, "mongodb": 1, "queue": 4}
	for rt, want := range wantTargets {
		if got := m.targets[rt]; got != want {
			t.Errorf("targets[%q] = %d, want %d", rt, got, want)
		}
	}
}
