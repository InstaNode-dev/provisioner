package redis

// Package redis handles Redis namespace provisioning.
// Supports two backends:
//   - "local": shared Redis with key-namespace isolation (prefix {token}:*)
//   - "upstash": Upstash REST API (creates isolated database) — stubbed for now

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

const defaultRedisAddr = "localhost:6379"

// aclAllowlist is the safe command allowlist applied to every provisioned ACL
// user on the shared Redis backend. It replaces "+@all" which would grant
// dangerous cross-tenant commands such as FLUSHDB, MONITOR, and CONFIG SET.
//
// Design rationale (§3 of DESIGN-P1-A-tier-enforcement.md):
//   - "+@all" on a shared pod allows FLUSHDB (wipes ALL tenants' data),
//     MONITOR (leaks all tenant commands in real time), and CONFIG SET
//     (removes pod-wide memory cap) — multi-tenant isolation failures.
//   - The key-pattern restriction (~{token}:*) does NOT cover admin/dangerous
//     commands; those operate at the server level, not the key level.
//   - "+@scripting" is included so Lua scripts work; Lua calling FLUSHDB is
//     mitigated by the explicit "-flushdb"/"-flushall" deny entries that Redis
//     evaluates before command execution.
//   - "-keys" removes the O(N) cross-tenant key scan; tenants should use SCAN.
var aclAllowlist = []interface{}{
	"+@read",        // GET, MGET, STRLEN, LRANGE, SMEMBERS, HGET, etc.
	"+@write",       // SET, MSET, DEL, LPUSH, SADD, HSET, etc.
	"+@string",      // Explicit string family (belt-and-suspenders with @read/@write)
	"+@hash",        // HSET, HGET, HMSET, etc.
	"+@list",        // LPUSH, LRANGE, etc.
	"+@set",         // SADD, SMEMBERS, etc.
	"+@sortedset",   // ZADD, ZRANGE, etc.
	"+@stream",      // XADD, XREAD — needed for stream workloads
	"+@hyperloglog", // PFADD, PFCOUNT
	"+@geo",         // GEOADD, GEODIST
	"+@pubsub",      // SUBSCRIBE, PUBLISH — needed for pub/sub workloads
	"+@scripting",   // EVAL, EVALSHA — Lua scripting; explicit denies below guard FLUSHDB via Lua
	"-@admin",       // FLUSHALL, DEBUG, SAVE, BGSAVE, CONFIG, etc.
	"-@dangerous",   // MONITOR, KEYS, OBJECT, SORT with STORE, MIGRATE
	"-config",       // CONFIG GET/SET/RESETSTAT — explicit deny even if @admin missed
	"-debug",        // DEBUG SLEEP, DEBUG JMAP
	"-monitor",      // MONITOR — explicit deny (cross-tenant command stream)
	"-flushdb",      // FLUSHDB — explicit deny (wipes ALL tenant data on shared pod)
	"-flushall",     // FLUSHALL — explicit deny
	"-acl",          // ACL SETUSER/DELUSER — prevents ACL self-escalation
	"-keys",         // KEYS — O(N) cross-tenant key scan; tenants must use SCAN
}

// LocalBackend provisions Redis namespaces on a shared Redis instance.
type LocalBackend struct {
	rdb       *goredis.Client // admin connection
	redisHost string          // Redis host for building connection strings
}

// newLocalBackend creates a LocalBackend connecting to the given redisHost.
// redisHost format: "host:port" (e.g. "localhost:6379").
func newLocalBackend(redisHost string) *LocalBackend {
	if redisHost == "" {
		redisHost = defaultRedisAddr
	}
	rdb := goredis.NewClient(&goredis.Options{
		Addr: redisHost,
	})
	// Extract just the host portion for URL building (strip port if needed for URL).
	return &LocalBackend{rdb: rdb, redisHost: redisHost}
}

// Provision creates a namespaced Redis "database" for the given token.
// Tries Redis ACL (Redis 6+) first. Falls back to key-namespace isolation
// if ACL is unavailable or disabled.
func (b *LocalBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	short := token
	if len(short) > 8 {
		short = short[:8]
	}
	username := fmt.Sprintf("usr_%s", short)
	keyPrefix := fmt.Sprintf("%s:", token)

	// Generate a random password for the ACL user.
	pwBytes := make([]byte, 16)
	if _, err := rand.Read(pwBytes); err != nil {
		return nil, fmt.Errorf("cache.provisionLocal: generate password: %w", err)
	}
	password := hex.EncodeToString(pwBytes)

	// Try ACL SETUSER (Redis 6+).
	// Pattern: <token>:* restricts key access to this token's namespace.
	// aclAllowlist replaces "+@all": on a shared pod, "+@all" grants
	// FLUSHDB/MONITOR/CONFIG which are multi-tenant isolation failures.
	// See aclAllowlist declaration for full rationale.
	aclArgs := []interface{}{"ACL", "SETUSER", username, "on", ">" + password, "~" + keyPrefix + "*"}
	aclArgs = append(aclArgs, aclAllowlist...)
	aclCmd := b.rdb.Do(ctx, aclArgs...)
	// userHost is what we embed in the URL returned to clients.
	// Falls back to the cluster-internal redisHost when no public host is configured.
	userHost := publicHostPort()
	if userHost == "" {
		userHost = b.redisHost
	}

	if aclCmd.Err() == nil {
		// ACL succeeded — return an isolated user URL.
		// KeyPrefix is returned so callers (and pool consumers) know which key namespace
		// this user is permitted to access (the ACL restricts to keyPrefix+"*").
		url := fmt.Sprintf("redis://%s:%s@%s/0", username, password, userHost)
		return &Credentials{
			URL:       url,
			KeyPrefix: keyPrefix,
		}, nil
	}

	// ACL failed (Redis < 6 or ACL disabled) — fall back to key-namespace isolation.
	// Return the shared Redis URL. Client must prefix all keys with {token}: to
	// stay in their namespace.
	url := fmt.Sprintf("redis://%s/0", userHost)
	return &Credentials{
		URL:       url,
		KeyPrefix: keyPrefix,
	}, nil
}

// publicHostPort returns the host:port to embed in user-facing Redis URLs.
// Resolution order:
//  1. REDIS_PUBLIC_HOST_PORT (e.g. "redis.instanode.dev:6379")
//  2. REDIS_PUBLIC_HOST + REDIS_PUBLIC_PORT (port defaults to 6379)
//  3. "" — caller falls back to the in-cluster redisHost
func publicHostPort() string {
	if hp := os.Getenv("REDIS_PUBLIC_HOST_PORT"); hp != "" {
		return hp
	}
	host := os.Getenv("REDIS_PUBLIC_HOST")
	if host == "" {
		return ""
	}
	port := os.Getenv("REDIS_PUBLIC_PORT")
	if port == "" {
		port = "6379"
	}
	return host + ":" + port
}

// StorageBytes returns the estimated memory used by keys with the token prefix.
// Iterates with SCAN MATCH "{token}:*" COUNT 100, sums MEMORY USAGE for each key.
// Capped at 1000 keys to avoid blocking the Redis event loop.
func (b *LocalBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	prefix := token + ":*"
	const maxKeys = 1000

	var (
		cursor     uint64
		totalKeys  int
		totalBytes int64
	)

	for {
		keys, nextCursor, err := b.rdb.Scan(ctx, cursor, prefix, 100).Result()
		if err != nil {
			return 0, fmt.Errorf("cache.StorageBytes scan: %w", err)
		}

		for _, key := range keys {
			if totalKeys >= maxKeys {
				break
			}
			totalKeys++

			// MEMORY USAGE returns bytes used by the key including metadata.
			// Err is non-nil if the key doesn't exist (just deleted).
			mem, err := b.rdb.MemoryUsage(ctx, key).Result()
			if err != nil {
				// Key was deleted between SCAN and MEMORY USAGE — skip it.
				if strings.Contains(err.Error(), "ERR") || err == goredis.Nil {
					continue
				}
				continue
			}
			totalBytes += mem
		}

		cursor = nextCursor
		if cursor == 0 || totalKeys >= maxKeys {
			break
		}
	}

	return totalBytes, nil
}

// Deprovision removes all keys in the token's namespace and the ACL user if it exists.
func (b *LocalBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	// Delete ACL user if it exists.
	short := token
	if len(short) > 8 {
		short = short[:8]
	}
	username := fmt.Sprintf("usr_%s", short)
	// Best-effort ACL delete — ignore errors (user may not exist or ACL may be disabled).
	b.rdb.Do(ctx, "ACL", "DELUSER", username)

	// Flush all keys in the token's namespace using SCAN + DEL.
	prefix := token + ":*"
	var cursor uint64
	for {
		keys, nextCursor, err := b.rdb.Scan(ctx, cursor, prefix, 100).Result()
		if err != nil {
			return fmt.Errorf("cache.Deprovision scan: %w", err)
		}
		if len(keys) > 0 {
			if err := b.rdb.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("cache.Deprovision del: %w", err)
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}
