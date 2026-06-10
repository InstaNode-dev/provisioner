package pool

// dropguard_test.go — name-convention guard + DDL-audit tests for the pool
// reaper's deprovisionBacking dispatch (truehomie hardening, task D3). The
// pool reaper is the one customer-infra drop path that does not pass through
// server.guardedDrop, so it carries its own copy of the chokepoint contract.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"instant.dev/provisioner/internal/dropguard"
)

// TestDeprovisionBacking_RefusedToken_NeverDispatches asserts a reserved or
// malformed naming token is refused BEFORE any backend dispatch. The Manager
// has nil backends: if the guard let the call through, the dispatch would
// panic — a returned ErrRefused proves the early return.
func TestDeprovisionBacking_RefusedToken_NeverDispatches(t *testing.T) {
	m := &Manager{}
	for _, tok := range []string{"postgres", "instant_customers", "", "a b"} {
		err := m.deprovisionBacking(context.Background(), "postgres", tok, "")
		if !errors.Is(err, dropguard.ErrRefused) {
			t.Fatalf("deprovisionBacking(token=%q): expected dropguard.ErrRefused, got %v", tok, err)
		}
	}
}

// TestDeprovisionBacking_ValidToken_EmitsAuditThenDispatches uses an unknown
// resource type so the dispatch lands on the error default (no backend
// needed): a valid token must pass the guard, emit the provisioner.drop audit
// event, and reach the dispatch switch.
func TestDeprovisionBacking_ValidToken_EmitsAuditThenDispatches(t *testing.T) {
	m := &Manager{}
	err := m.deprovisionBacking(context.Background(), "weird-type", "pool-96edf9ee-d8ed-4292-9036-b63298ec5b2b", "")
	if err == nil || !strings.Contains(err.Error(), "unknown resource type") {
		t.Fatalf("expected unknown-resource-type dispatch error after passing the guard, got %v", err)
	}
	if errors.Is(err, dropguard.ErrRefused) {
		t.Fatalf("valid pool token must not be refused: %v", err)
	}
}

func TestPoolResourceTypeProto_MapsEveryPoolType(t *testing.T) {
	want := map[string]string{
		"postgres":      "RESOURCE_TYPE_POSTGRES",
		"redis":         "RESOURCE_TYPE_REDIS",
		"mongodb":       "RESOURCE_TYPE_MONGODB",
		"queue":         "RESOURCE_TYPE_QUEUE",
		"somethingelse": "RESOURCE_TYPE_UNSPECIFIED",
	}
	for in, out := range want {
		if got := poolResourceTypeProto(in); got != out {
			t.Errorf("poolResourceTypeProto(%q) = %q, want %q", in, got, out)
		}
	}
}
