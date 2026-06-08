package server_test

// server_redis_tier_routing_test.go — wiring tests for the
// REDIS_TIER_AWARE_ROUTING_ENABLED flag in server.New.
//
// These tests prove the NO-OP guarantee: with the flag OFF (the default), the
// server's shared redis backend is exactly the backend selected by
// REDIS_PROVISION_BACKEND — NOT wrapped in a dispatcher — so every tier
// (including team) flows through the configured backend exactly as before. With
// the flag ON, the backend is wrapped in a redis.TierDispatchBackend.
//
// The dispatcher's per-tier routing itself is covered by the pure unit tests in
// internal/backend/redis/dispatch_test.go; here we only assert the flag controls
// whether that dispatcher is installed at all.

import (
	"testing"

	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/server"
)

// baseRoutingCfg returns a config whose backends never dial anything, so
// server.New can be exercised offline.
func baseRoutingCfg() *config.Config {
	return &config.Config{
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
	}
}

// TestNew_TierRoutingDisabled_BackendUnwrapped is the no-op proof: when the flag
// is unset/false the shared redis backend is NOT a TierDispatchBackend — it is
// the configured backend verbatim, so behaviour is identical to today.
func TestNew_TierRoutingDisabled_BackendUnwrapped(t *testing.T) {
	cfg := baseRoutingCfg()
	cfg.RedisTierAwareRoutingEnabled = false // explicit; this is also the default

	srv := server.New(cfg, nil)
	if srv == nil {
		t.Fatal("server.New returned nil")
	}
	if server.RedisBackendIsTierDispatch(srv) {
		t.Fatalf("flag OFF but redis backend is a TierDispatchBackend (%s) — must be the configured backend, unwrapped",
			server.RedisBackendTypeName(srv))
	}
}

// TestNew_TierRoutingDefaultIsDisabled guards the default: a zero-value config
// (flag unset) must NOT wrap the backend. This is the "safe to merge, no-op in
// prod" guarantee — a deploy that does not set the env var changes nothing.
func TestNew_TierRoutingDefaultIsDisabled(t *testing.T) {
	cfg := baseRoutingCfg() // RedisTierAwareRoutingEnabled left at its zero value (false)

	srv := server.New(cfg, nil)
	if server.RedisBackendIsTierDispatch(srv) {
		t.Fatalf("default config wrapped the redis backend (%s) — tier-aware routing must be opt-in",
			server.RedisBackendTypeName(srv))
	}
}

// TestNew_TierRoutingEnabled_BackendWrapped asserts the flag installs the
// dispatcher.
func TestNew_TierRoutingEnabled_BackendWrapped(t *testing.T) {
	cfg := baseRoutingCfg()
	cfg.RedisTierAwareRoutingEnabled = true

	srv := server.New(cfg, nil)
	if srv == nil {
		t.Fatal("server.New returned nil")
	}
	if !server.RedisBackendIsTierDispatch(srv) {
		t.Fatalf("flag ON but redis backend is %s — expected a TierDispatchBackend",
			server.RedisBackendTypeName(srv))
	}
}
