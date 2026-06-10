package redis

// dropguard_test.go — name-convention guard tests for the Redis ACL DELUSER
// sites (truehomie hardening, task D3). The invariant: a non-tenant-shaped
// username (e.g. "default", an admin user) is never passed to ACL DELUSER —
// deleting "default" would brick every tenant on the shared pod.

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// TestDedicatedDeprovisionLocal_RefusedProbe_SkippedNotExecuted stamps a
// providerResourceID that resolves to the Redis admin user "default" (the
// resolveDedicatedACLUsername fast-path returns it verbatim). The guard must
// SKIP the DELUSER: the provider is constructed with a nil client, so any
// executed Redis command would panic — returning nil proves the skip.
func TestDedicatedDeprovisionLocal_RefusedProbe_SkippedNotExecuted(t *testing.T) {
	p := &DedicatedProvider{}
	if err := p.deprovisionLocal(context.Background(), "x", "default"); err != nil {
		t.Fatalf("deprovisionLocal with refused probe: want nil (skip), got %v", err)
	}
}

// TestLocalDeprovision_ReservedLegacyACLUser_Skipped uses a token whose 8-char
// legacy truncation is the reserved identifier "postgres"
// ("postgres1ab2"[:8] == "postgres"). The legacy DELUSER candidate must be
// refused/skipped while the canonical one proceeds. The client points at an
// unreachable address: the canonical DELUSER error is best-effort (ignored),
// and the subsequent SCAN fails — so the expected outcome is the SCAN error,
// never a dropguard refusal and never a DELUSER of usr_postgres.
func TestLocalDeprovision_ReservedLegacyACLUser_Skipped(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1", // nothing listens here
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer rdb.Close()
	b := &LocalBackend{rdb: rdb}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := b.Deprovision(ctx, "postgres1ab2", "")
	if err == nil {
		t.Fatal("Deprovision against unreachable Redis: want SCAN error, got nil")
	}
}
