package redis

// dedicated.go — DedicatedProvider provisions a dedicated Redis instance for the Team tier.
//
// Two modes:
//  1. Local admin mode (upstashAPIKey == ""): creates an ACL user on a separate
//     "dedicated" Redis cluster with full keyspace access (no prefix restriction).
//     This is the local/dev path — the dedicated Redis is pointed to by adminRedisURL.
//  2. Upstash API mode (upstashAPIKey != ""): calls the Upstash REST API to create
//     an isolated database (stubbed — wires the API shape but always returns
//     errUpstashNotImplemented until the key is set in production).

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// DedicatedProvider provisions a dedicated Redis instance.
// For local dev: creates an ACL user on a "dedicated" Redis with full keyspace access.
// For production: would call Upstash API or provision a new Redis container.
type DedicatedProvider struct {
	adminRedisURL string // redis://[password@]host:port — dedicated Redis cluster
	upstashAPIKey string // optional: Upstash REST API key
	rdb           *goredis.Client
	redisHost     string // host:port extracted from adminRedisURL for building conn strings
}

// NewDedicatedProvider creates a DedicatedProvider.
// adminRedisURL must be in the form redis://[password@]host:port or
// redis://:password@host:port.
// upstashAPIKey is optional; when set the Upstash API path is used.
func NewDedicatedProvider(adminRedisURL, upstashAPIKey string) *DedicatedProvider {
	if adminRedisURL == "" {
		adminRedisURL = "redis://localhost:6379"
	}
	opts, err := goredis.ParseURL(adminRedisURL)
	if err != nil {
		// Fallback: treat as plain host:port.
		opts = &goredis.Options{Addr: adminRedisURL}
	}
	rdb := goredis.NewClient(opts)
	host := opts.Addr

	return &DedicatedProvider{
		adminRedisURL: adminRedisURL,
		upstashAPIKey: upstashAPIKey,
		rdb:           rdb,
		redisHost:     host,
	}
}

// Provision creates a dedicated Redis instance for the given token.
func (p *DedicatedProvider) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	if p.upstashAPIKey != "" {
		return p.provisionUpstash(ctx, token, tier)
	}
	return p.provisionLocal(ctx, token, tier)
}

// StorageBytes returns memory used by the dedicated Redis instance.
func (p *DedicatedProvider) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	if p.upstashAPIKey != "" {
		// Upstash reports per-database stats via its REST API.
		// Not yet implemented — return 0 fail-open.
		slog.Warn("cache.dedicated.StorageBytes: upstash stats not implemented, returning 0",
			"token", token, "provider_resource_id", providerResourceID)
		return 0, nil
	}
	return p.localStorageBytes(ctx, token)
}

// Deprovision tears down the dedicated Redis instance.
//
// providerResourceID is the canonical ACL username stamped at provision time
// (see dedident.go). Deprovision deletes that username and ALSO probes the
// legacy ded_<token[:8]> form so an ACL user created before the token-
// truncation fix shipped is still cleaned up.
func (p *DedicatedProvider) Deprovision(ctx context.Context, token, providerResourceID string) error {
	if p.upstashAPIKey != "" {
		return p.deprovisionUpstash(ctx, token, providerResourceID)
	}
	return p.deprovisionLocal(ctx, token, providerResourceID)
}

// --- Upstash path (stubbed) ---

// provisionUpstash is the Upstash API path.  The Upstash REST API shape is
// documented at https://upstash.com/docs/redis/api/create-database but the
// integration is not yet live.  The stub is wired so that adding the API call
// is a surgical change when the key is set.
func (p *DedicatedProvider) provisionUpstash(ctx context.Context, token, tier string) (*Credentials, error) {
	// TODO: POST https://api.upstash.com/v2/redis/database
	// body: {"name":"instant-{token}","region":"us-east-1","tls":true}
	// response: {"endpoint":"...", "password":"...", "port":...}
	return nil, fmt.Errorf("cache.dedicated.provisionUpstash: upstash integration not yet implemented — set DEDICATED_REDIS_URL for local mode")
}

func (p *DedicatedProvider) deprovisionUpstash(ctx context.Context, token, providerResourceID string) error {
	// TODO: DELETE https://api.upstash.com/v2/redis/database/{providerResourceID}
	return fmt.Errorf("cache.dedicated.deprovisionUpstash: upstash integration not yet implemented")
}

// --- Local admin path (dev/test) ---

// provisionLocal creates an ACL user on the dedicated Redis with whole-keyspace
// access. Unlike the shared backend (which restricts to {token}:* prefix), a
// dedicated user gets ~* (all keys) because the entire Redis instance is theirs.
//
// Command grants are still scoped: aclAllowlist (the shared backend's command
// allowlist) replaces "+@all". "+@all" grants FLUSHALL, CONFIG SET (defeats the
// tier memory cap), MONITOR and DEBUG — dangerous even on a "dedicated" instance,
// which in local dev is in fact shared (see deprovisionLocal). See P1-B.
func (p *DedicatedProvider) provisionLocal(ctx context.Context, token, tier string) (*Credentials, error) {
	// Token-truncation fix (P1, BUGHUNT-REPORT-2026-05-17-round2): the ACL
	// username uses the FULL token via dedicatedACLUsername — never the old
	// ded_<token[:8]> truncation, which let two tokens sharing 8 hex chars
	// collide on one ACL user. The canonical username is also returned as
	// ProviderResourceID so every lifecycle RPC resolves it from the stored
	// value instead of re-deriving. See dedident.go.
	username := dedicatedACLUsername(token)

	pwBytes := make([]byte, 16)
	if _, err := rand.Read(pwBytes); err != nil {
		return nil, fmt.Errorf("cache.dedicated.provisionLocal: generate password: %w", err)
	}
	password := hex.EncodeToString(pwBytes)

	// Whole-keyspace key/channel access (~* &*) — the instance is theirs — but
	// the command set is scoped via aclAllowlist, dropping FLUSHALL/FLUSHDB/
	// CONFIG/MONITOR/DEBUG/ACL/KEYS. Reuses the shared backend's allowlist so
	// the two paths can never drift.
	aclArgs := []interface{}{"ACL", "SETUSER", username,
		"on",
		">" + password,
		"~*", // all keys — dedicated instance
		"&*", // all channels
	}
	aclArgs = append(aclArgs, aclAllowlist...)
	aclCmd := p.rdb.Do(ctx, aclArgs...)
	if aclCmd.Err() != nil {
		// ACL unavailable — fall back to returning the admin URL directly.
		// This degrades gracefully on older Redis versions.
		slog.Warn("cache.dedicated.provisionLocal: ACL SETUSER failed, returning admin URL",
			"token", token, "error", aclCmd.Err())
		url := fmt.Sprintf("redis://%s/0", p.redisHost)
		// No ACL user was created — there is no per-resource identifier to
		// persist, so ProviderResourceID stays empty (lifecycle RPCs then
		// fall back to the canonical full-token derivation).
		return &Credentials{
			URL:       url,
			KeyPrefix: "", // no prefix restriction
		}, nil
	}

	url := fmt.Sprintf("redis://%s:%s@%s/0", username, password, p.redisHost)
	slog.Info("cache.dedicated.provisionLocal: provisioned",
		"token", token,
		"user", username,
		"tier", tier,
	)
	return &Credentials{
		URL:       url,
		KeyPrefix: "", // no prefix restriction for dedicated
		// Persist the EXACT ACL username created above so Deprovision /
		// StorageBytes resolve it from the stored value, never re-derive it.
		ProviderResourceID: username,
	}, nil
}

// localStorageBytes returns the total memory used by the dedicated Redis
// instance via INFO memory (reports used_memory for the whole instance).
//
// P2-09 (KNOWN LIMITATION, not fixed here — requires a design change):
// In a TRUE dedicated deployment (one Redis instance per Team-tier token) the
// whole-instance used_memory IS the per-tenant figure — there is exactly one
// tenant, so this is correct. In local-dev "shared dedicated" mode (multiple
// tokens' ACL users on one Redis, see deprovisionLocal) every tenant reports
// the same inflated whole-instance number. A per-tenant SCAN+MEMORY USAGE pass
// is NOT possible for the dedicated provider because it grants each ACL user
// "~*" (whole keyspace) with no key prefix — there is no prefix to SCAN by, by
// design (the instance is meant to be theirs alone). Scoping per-tenant would
// require assigning prefixes to dedicated users, which would defeat the
// "dedicated = whole keyspace" contract. The shared/prefixed backend
// (local.go) measures correctly via its {token}:* prefix; the dedicated
// provider is accurate only in real one-instance-per-tenant production.
func (p *DedicatedProvider) localStorageBytes(ctx context.Context, token string) (int64, error) {
	info, err := p.rdb.Info(ctx, "memory").Result()
	if err != nil {
		return 0, fmt.Errorf("cache.dedicated.localStorageBytes: INFO memory: %w", err)
	}
	// Parse used_memory from INFO output.
	const key = "used_memory:"
	for _, line := range splitLines(info) {
		if len(line) > len(key) && line[:len(key)] == key {
			var size int64
			if _, err := fmt.Sscanf(line[len(key):], "%d", &size); err == nil {
				return size, nil
			}
		}
	}
	return 0, nil
}

// deprovisionLocal removes the ACL user from the dedicated Redis.
// It does NOT flush the database — that would disrupt other users on a shared
// dedicated Redis in local dev.  For a real dedicated instance the entire
// instance would be destroyed instead.
//
// It deletes the canonical username (resolved from providerResourceID, the
// value stamped at provision time) AND probes the legacy ded_<token[:8]> form
// so an ACL user created before the token-truncation fix is still cleaned up.
// DELUSER is idempotent — deleting a non-existent user is a Redis no-op — so
// probing both names is always safe.
func (p *DedicatedProvider) deprovisionLocal(ctx context.Context, token, providerResourceID string) error {
	canonical := resolveDedicatedACLUsername(token, providerResourceID)
	legacy := legacyDedicatedACLUsername(token)

	// Probe the canonical name, plus the legacy 8-char name when it is
	// non-empty and distinct. DELUSER is idempotent (non-existent user → Redis
	// no-op), so probing both is always safe.
	probes := []string{canonical}
	if legacy != "" && legacy != canonical {
		probes = append(probes, legacy)
	}
	for _, username := range probes {
		if err := p.rdb.Do(ctx, "ACL", "DELUSER", username).Err(); err != nil {
			// Best-effort — user may not exist.
			slog.Warn("cache.dedicated.deprovisionLocal: ACL DELUSER (non-fatal)",
				"token", token, "user", username, "error", err)
		}
	}
	slog.Info("cache.dedicated.deprovisionLocal: deprovisioned",
		"token", token, "user", canonical)
	return nil
}

// splitLines splits a string by newlines (handles \r\n and \n).
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
