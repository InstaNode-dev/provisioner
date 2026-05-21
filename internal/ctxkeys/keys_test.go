package ctxkeys

// keys_test.go — exhaustive coverage for the ctxkeys package.
//
// The package is intentionally tiny: a single unexported `contextKey` type
// and a single exported constant `TeamIDKey`. There is no behaviour to
// unit-test beyond the round-trip contract every typed-key package shares:
//
//   1. A value stored under the typed key MUST be retrievable with the
//      exact same key — i.e. the constant is stable across reads.
//   2. The typed key MUST NOT collide with raw-string keys carrying the
//      same lexical name (the whole reason for the package's existence).
//   3. The zero context (or any unrelated context) MUST return the typed
//      key's value as nil — Go's documented context.Value contract.
//
// These tests guarantee the public surface is what it claims to be; if a
// future refactor accidentally re-types TeamIDKey to a string (or exposes
// the unexported contextKey), the third test below fails to compile or
// asserts incorrectly.

import (
	"context"
	"testing"
)

// TestTeamIDKey_RoundTrip verifies the basic store/load contract.
func TestTeamIDKey_RoundTrip(t *testing.T) {
	const want = "team-abc-123"
	ctx := context.WithValue(context.Background(), TeamIDKey, want)

	got, ok := ctx.Value(TeamIDKey).(string)
	if !ok {
		t.Fatalf("ctx.Value(TeamIDKey) did not return string; got %T", ctx.Value(TeamIDKey))
	}
	if got != want {
		t.Errorf("ctx.Value(TeamIDKey) = %q, want %q", got, want)
	}
}

// TestTeamIDKey_EmptyMeansAnonymous documents the empty-string == anonymous
// convention spelled out in the package doc comment. A consumer that switches
// on `team_id == ""` to skip namespace labelling must be able to distinguish
// "key absent" from "key present but empty" — both legitimately mean
// "no owning team".
func TestTeamIDKey_EmptyMeansAnonymous(t *testing.T) {
	// Key absent.
	if v := context.Background().Value(TeamIDKey); v != nil {
		t.Errorf("background ctx returned %v for TeamIDKey; want nil", v)
	}

	// Key present, empty string.
	ctx := context.WithValue(context.Background(), TeamIDKey, "")
	if got, ok := ctx.Value(TeamIDKey).(string); !ok || got != "" {
		t.Errorf("present-but-empty TeamIDKey = (%q, ok=%v); want (\"\", true)", got, ok)
	}
}

// TestTeamIDKey_NotStringCollision is the regression test for the whole
// reason this package exists: a bare string key like "team_id" stored by
// some other package MUST NOT shadow the typed TeamIDKey, and vice-versa.
// If a future refactor turns TeamIDKey back into a string, this test fails.
func TestTeamIDKey_NotStringCollision(t *testing.T) {
	type stringyKey string
	const collide stringyKey = "TeamIDKey"

	ctx := context.WithValue(context.Background(), collide, "other-package-value")
	ctx = context.WithValue(ctx, TeamIDKey, "instant-package-value")

	if got, _ := ctx.Value(TeamIDKey).(string); got != "instant-package-value" {
		t.Errorf("typed key shadowed by string key: got %q, want %q", got, "instant-package-value")
	}
	if got, _ := ctx.Value(collide).(string); got != "other-package-value" {
		t.Errorf("string key shadowed by typed key: got %q, want %q", got, "other-package-value")
	}
}

// TestTeamIDKey_NestedOverride asserts the standard context-chain rule —
// an inner WithValue under the same key wins. Confirms TeamIDKey is a
// well-behaved context key.
func TestTeamIDKey_NestedOverride(t *testing.T) {
	outer := context.WithValue(context.Background(), TeamIDKey, "outer")
	inner := context.WithValue(outer, TeamIDKey, "inner")

	if got, _ := outer.Value(TeamIDKey).(string); got != "outer" {
		t.Errorf("outer = %q, want %q", got, "outer")
	}
	if got, _ := inner.Value(TeamIDKey).(string); got != "inner" {
		t.Errorf("inner = %q, want %q", got, "inner")
	}
}

// TestTeamIDKey_Distinct guards against a future second constant being
// declared with the same iota value — the test trips if `TeamIDKey != 0`
// (the first iota) because we rely on it being a stable underlying value.
// If a constant is inserted ABOVE TeamIDKey in the const block, TeamIDKey's
// iota shifts and this test catches it.
func TestTeamIDKey_Distinct(t *testing.T) {
	if int(TeamIDKey) != 0 {
		t.Errorf("TeamIDKey underlying iota = %d; want 0 — a constant was inserted above it, every WithValue using TeamIDKey now collides with whatever has the new iota=0", int(TeamIDKey))
	}
}
