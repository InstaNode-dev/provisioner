package mongo

// dropguard_test.go — name-convention guard tests for the Mongo Deprovision
// path (truehomie hardening, task D3). The invariant: a reserved/system naming
// token is refused BEFORE any connection, and a reserved derived candidate name
// is never passed to dropUser/dropDatabase.

import (
	"context"
	"errors"
	"testing"
	"time"

	"instant.dev/provisioner/internal/dropguard"
)

// TestLocalDeprovision_RefusedToken_NeverConnects points the backend at an
// unreachable URI: a refused token must return dropguard.ErrRefused instantly,
// proving the guard runs before connect (a connect attempt would instead
// return a server-selection error after the timeout).
func TestLocalDeprovision_RefusedToken_NeverConnects(t *testing.T) {
	b := newLocalBackend("mongodb://127.0.0.1:1", "127.0.0.1:1")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, tok := range []string{"postgres", "instant_customers", "", "admin", "a b"} {
		err := b.Deprovision(ctx, tok, "")
		if !errors.Is(err, dropguard.ErrRefused) {
			t.Fatalf("Deprovision(%q): expected dropguard.ErrRefused before connect, got %v", tok, err)
		}
	}
}

// TestLocalDeprovision_ReservedLegacyCandidate_SkippedNotFatal uses a token
// whose 12-char legacy truncation collides with a reserved identifier
// ("instant_customersabc"[:12] == "instant_cust"). The legacy candidate must
// be SKIPPED (refused, logged) while the canonical candidate proceeds — the
// deprovision still succeeds.
func TestLocalDeprovision_ReservedLegacyCandidate_SkippedNotFatal(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Deprovision(ctx, "instant_customersabc", ""); err != nil {
		t.Fatalf("Deprovision with reserved legacy candidate: want nil (skip), got %v", err)
	}
}

// TestLocalDeprovision_ReservedCanonicalDB_ReturnsError uses a token that
// passes the token guard ("ad-min") but whose canonical dash-stripped form is
// the reserved identifier "admin" — so the CANONICAL candidates (usr_admin,
// db_admin) embed a reserved tail. A refused canonical DB candidate is a hard
// error (unlike a legacy candidate, which is skipped): the deprovision must
// fail loudly rather than silently leak the real database.
func TestLocalDeprovision_ReservedCanonicalDB_ReturnsError(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := b.Deprovision(ctx, "ad-min", "")
	if !errors.Is(err, dropguard.ErrRefused) {
		t.Fatalf("Deprovision(%q): expected dropguard.ErrRefused for reserved canonical db candidate, got %v", "ad-min", err)
	}
}
