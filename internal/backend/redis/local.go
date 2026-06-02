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
	"log/slog"
	"os"

	goredis "github.com/redis/go-redis/v9"

	"instant.dev/provisioner/internal/poolident"
)

const defaultRedisAddr = "localhost:6379"

// aclUserPrefix is the prefix for every ACL username provisioned on the shared
// Redis backend. The username is aclUserPrefix + the FULL token (matching the
// key-prefix which also uses the full token). Using the full token avoids the
// cross-tenant collision that an 8-char truncation caused: two tokens sharing
// their first 8 hex chars would map to the same ACL user, so an ACL SETUSER
// upsert silently overwrote tenant A's credentials and a later ACL DELUSER
// revoked the survivor. See P1-D.
const aclUserPrefix = "usr_"

// legacyACLUserShortLen is the truncation length used by the pre-P1-D username
// scheme (aclUserPrefix + token[:8]). Deprovision still probes this form so
// ACL users created before the fix are cleaned up. New provisions never use it.
const legacyACLUserShortLen = 8

// aclUsername returns the canonical ACL username for a token: the full token,
// not a truncated prefix, so two tokens can never collide on the same user.
func aclUsername(token string) string {
	return aclUserPrefix + token
}

// legacyACLUsername returns the pre-P1-D 8-char-prefix username for a token,
// or "" when the token is too short to have been truncated (in which case the
// canonical name already equals the legacy name and no extra probe is needed).
func legacyACLUsername(token string) string {
	if len(token) <= legacyACLUserShortLen {
		return ""
	}
	return aclUserPrefix + token[:legacyACLUserShortLen]
}

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
	// Username is derived from the FULL token (matching keyPrefix). Truncating
	// to 8 chars let two tokens collide on the same ACL user — see P1-D.
	username := aclUsername(token)
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

	// ACL SETUSER failed. The LocalBackend serves the SHARED, multi-tenant
	// redis-provision pool whose default user is nopass/+@all — so the old
	// credential-less "redis://host/0" fallback handed this tenant UNRESTRICTED
	// access to every other tenant's keys (the KeyPrefix is advisory, NOT
	// enforced). That is a cross-tenant isolation failure, so we now fail
	// CLOSED on the shared backend (bug bash 2026-06-02 #19).
	//
	// The credential-less, key-namespace-only fallback is only acceptable on a
	// genuinely single-tenant deployment (or Redis < 6 with no ACL support in
	// dev). Such deployments opt in explicitly via
	// REDIS_ALLOW_INSECURE_NO_ACL_FALLBACK=true; prod leaves it unset and gets
	// a hard error instead of a silent isolation downgrade.
	if os.Getenv("REDIS_ALLOW_INSECURE_NO_ACL_FALLBACK") == "true" {
		slog.Warn("cache.provisionLocal: ACL SETUSER failed — returning INSECURE credential-less shared URL (REDIS_ALLOW_INSECURE_NO_ACL_FALLBACK=true; no enforced cross-tenant isolation)",
			"error", aclCmd.Err())
		url := fmt.Sprintf("redis://%s/0", userHost)
		return &Credentials{
			URL:       url,
			KeyPrefix: keyPrefix,
		}, nil
	}
	return nil, fmt.Errorf("cache.provisionLocal: ACL SETUSER failed on shared multi-tenant Redis — refusing to return a credential-less shared URL (set REDIS_ALLOW_INSECURE_NO_ACL_FALLBACK=true only for single-tenant/dev): %w", aclCmd.Err())
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
//
// P0-2: a pool-claimed cache's keyspace is "pool-<uuid>:*", not "<real-token>:*".
// poolident.NamingToken resolves the canonical naming token from
// provider_resource_id so quota is measured against the real keyspace instead
// of silently reporting 0 bytes.
func (b *LocalBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	prefix := poolident.NamingToken(token, providerResourceID) + ":*"
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
			mem, err := b.rdb.MemoryUsage(ctx, key).Result()
			if err != nil {
				// Key deleted between SCAN and MEMORY USAGE — a benign race.
				// Redis returns a nil bulk reply for MEMORY USAGE on a missing
				// key, which go-redis maps to goredis.Nil. Skip ONLY that case
				// (bug bash 2026-06-02 #24 / security review HIGH): the old
				// `|| strings.Contains(err.Error(), "ERR")` clause added no
				// real race coverage and silently swallowed genuine server
				// errors ("ERR max number of clients reached", "ERR DENIED by
				// ACL"), under-counting quota and reporting a partial sum as
				// authoritative. Every other error propagates so quota is never
				// decided on partial data.
				if err == goredis.Nil {
					continue
				}
				// Any other error (conn drop, timeout, ctx-cancel) means the
				// remaining keys are unmeasured — a partial sum would be a
				// corrupt total reported as authoritative. Fail open (the
				// quota convention: the worker skips this tick) rather than
				// continuing with an under-count.
				return 0, fmt.Errorf("cache.StorageBytes memory usage: %w", err)
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
//
// Backward-compat (P1-D): new provisions name the ACL user from the full token
// (aclUsername), but resources provisioned before the fix used the truncated
// 8-char form (legacyACLUsername). Deprovision deletes BOTH candidate names —
// canonical first, legacy second — so pre-fix users are still cleaned up. The
// candidates are de-duplicated because, for short tokens, the two names are
// identical (legacyACLUsername returns "" in that case).
func (b *LocalBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	// P0-2: a pool-claimed cache's ACL user is "usr_pool-<uuid>" and its
	// keyspace "pool-<uuid>:*" — both named from the pool token, not the
	// request token. Resolve the canonical naming token from
	// provider_resource_id so DELUSER/SCAN target the real infra and it does
	// not leak forever.
	namingToken := poolident.NamingToken(token, providerResourceID)

	// Delete ACL user if it exists. Best-effort — ignore errors (user may not
	// exist or ACL may be disabled).
	for _, username := range []string{aclUsername(namingToken), legacyACLUsername(namingToken)} {
		if username == "" {
			continue
		}
		b.rdb.Do(ctx, "ACL", "DELUSER", username)
	}

	// Flush all keys in the token's namespace using SCAN + DEL.
	prefix := namingToken + ":*"
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
