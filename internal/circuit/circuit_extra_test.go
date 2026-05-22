package circuit

// circuit_extra_test.go — coverage for the constructor clamps, the nil-error
// arm of isCallerDeadline, and the onOpen callback firing on a half-open
// re-open (the existing suite uses NewBreaker without WithOnOpen, so that arm
// was never exercised).

import (
	"testing"
	"time"
)

// TestNewBreaker_ClampsBadConfig — a threshold < 1 snaps to 1 and a cooldown
// <= 0 snaps to 30s, rather than panicking on the provision hot path.
func TestNewBreaker_ClampsBadConfig(t *testing.T) {
	// threshold 0 → clamps to 1: a single failure must open the breaker.
	b := NewBreaker("clamp_threshold/"+t.Name(), 0, fastCooldown)
	_ = b.Allow()
	b.Record(errBoom)
	if b.State() != StateOpen {
		t.Fatalf("threshold<1 should clamp to 1 (one failure opens); state=%s", b.State())
	}

	// cooldown 0 → clamps to 30s: after opening, the breaker must stay open
	// well past a few ms (proving the 30s default, not a zero cooldown that
	// would immediately allow a trial).
	b2 := NewBreaker("clamp_cooldown/"+t.Name(), 1, 0)
	_ = b2.Allow()
	b2.Record(errBoom)
	if b2.State() != StateOpen {
		t.Fatalf("expected open, got %s", b2.State())
	}
	time.Sleep(15 * time.Millisecond)
	if b2.Allow() {
		t.Fatal("cooldown<=0 should clamp to 30s — a trial must NOT be admitted after 15ms")
	}
}

// TestIsCallerDeadline_Nil — the nil-error fast path returns false.
func TestIsCallerDeadline_Nil(t *testing.T) {
	if isCallerDeadline(nil) {
		t.Fatal("isCallerDeadline(nil) = true, want false")
	}
}

// TestRecord_HalfOpenReopen_FiresOnOpen — a half-open trial failure that
// re-opens the breaker must fire the onOpen callback. The default suite never
// installs onOpen, so this arm (the `if b.onOpen != nil` in the half-open
// re-open path) was uncovered.
func TestRecord_HalfOpenReopen_FiresOnOpen(t *testing.T) {
	var opens int
	b := NewBreaker("reopen_onopen/"+t.Name(), 1, fastCooldown).
		WithOnOpen(func() { opens++ })

	// Trip closed→open (fires onOpen once).
	_ = b.Allow()
	b.Record(errBoom)
	if opens != 1 {
		t.Fatalf("after first open, opens=%d want 1", opens)
	}

	// Cooldown, grab the half-open trial, fail it → re-open (fires onOpen again).
	time.Sleep(shortWait)
	if !b.Allow() {
		t.Fatal("trial Allow() should succeed after cooldown")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("expected half_open, got %s", b.State())
	}
	b.Record(errBoom)
	if b.State() != StateOpen {
		t.Fatalf("failed trial should re-open, got %s", b.State())
	}
	if opens != 2 {
		t.Fatalf("half-open re-open should fire onOpen again; opens=%d want 2", opens)
	}
}
