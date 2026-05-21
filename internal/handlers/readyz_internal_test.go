package handlers

// readyz_internal_test.go — whitebox tests for the defensive nil-pool branch
// in platformDBCheck. NewReadyzHandler only registers the platform_db check
// when h.pool != nil, so the `if h.pool == nil` branch inside the closure is
// dead under the public surface — but the guard is kept as belt-and-braces
// in case a future refactor wires the check unconditionally. A whitebox test
// pins the contract: "registered but pool nil" maps to failed +
// pgxpool_not_configured.

import (
	"context"
	"testing"

	"instant.dev/common/readiness"
)

func TestPlatformDBCheck_NilPool_DefensiveBranch(t *testing.T) {
	h := &ReadyzHandler{} // pool intentionally nil
	check := h.platformDBCheck()
	res := check(context.Background())
	if res.Status != readiness.StatusFailed {
		t.Errorf("nil-pool branch: status = %q, want failed", res.Status)
	}
	if res.LastError != "pgxpool_not_configured" {
		t.Errorf("nil-pool branch: LastError = %q, want pgxpool_not_configured", res.LastError)
	}
}
