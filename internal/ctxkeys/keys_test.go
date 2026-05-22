package ctxkeys

// keys_test.go — round-trip coverage for the typed context key. The key's whole
// purpose is collision-free storage of the owning team UUID on a context that
// crosses package boundaries (gRPC handler → k8s backends), so the test asserts
// (a) a value stored under TeamIDKey reads back unchanged, (b) a bare context
// yields the zero value, and (c) the unexported keyed type does not collide with
// a same-underlying-int key from another type.

import (
	"context"
	"testing"
)

func TestTeamIDKey_RoundTrip(t *testing.T) {
	const team = "team-abc-123"
	ctx := context.WithValue(context.Background(), TeamIDKey, team)

	got, ok := ctx.Value(TeamIDKey).(string)
	if !ok {
		t.Fatalf("value under TeamIDKey was not a string")
	}
	if got != team {
		t.Fatalf("round-trip = %q, want %q", got, team)
	}
}

func TestTeamIDKey_AbsentIsZero(t *testing.T) {
	if v := context.Background().Value(TeamIDKey); v != nil {
		t.Fatalf("bare context yielded %v under TeamIDKey, want nil", v)
	}
}

// TestTeamIDKey_NoCollisionWithRawInt — the typed contextKey(0) must NOT alias a
// plain int(0) key, which is the entire reason for the unexported named type.
func TestTeamIDKey_NoCollisionWithRawInt(t *testing.T) {
	ctx := context.WithValue(context.Background(), TeamIDKey, "scoped")
	// A raw int key with the same underlying value must miss.
	if v := ctx.Value(0); v != nil {
		t.Fatalf("raw int(0) key collided with TeamIDKey: got %v", v)
	}
}
