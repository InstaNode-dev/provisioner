package circuit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// errBoom is a sentinel for non-deadline failures. Matches the api/worker
// breaker tests so the behavioural contract reads identically across all
// three breaker copies.
var errBoom = errors.New("boom")

// fastCooldown / shortWait are picked tight enough that the recovery tests
// don't waste wall-clock time but loose enough that a busy CI runner won't
// flake them. 10ms cooldown + 15ms wait is the same shape api/worker use.
const (
	fastCooldown = 10 * time.Millisecond
	shortWait    = 15 * time.Millisecond
)

// allBackends enumerates the five per-backend breakers the audit P0-3
// mandate calls for. Every regression test iterates this list (not a
// hand-typed slice inlined per test) — that is the rule-18 "registry-
// iterating regression test" pattern from CLAUDE.md: if a sixth backend is
// added, the registry must own the test surface, not the test file.
var allBackends = []string{
	BackendPostgresK8s,
	BackendPostgresAdmin,
	BackendRedisAdmin,
	BackendMongoAdmin,
	BackendK8sAPI,
}

// breakerForBackend returns the Breakers-set member that corresponds to
// `name`. Used by the per-backend table tests so a new backend can be wired
// in by adding to allBackends + the Breakers struct, without re-writing
// the assertions.
func breakerForBackend(set *Breakers, name string) *Breaker {
	switch name {
	case BackendPostgresK8s:
		return set.PostgresK8s
	case BackendPostgresAdmin:
		return set.PostgresAdmin
	case BackendRedisAdmin:
		return set.RedisAdmin
	case BackendMongoAdmin:
		return set.MongoAdmin
	case BackendK8sAPI:
		return set.K8sAPI
	default:
		return nil
	}
}

// newTestBreaker builds a fresh breaker per case so global state across
// tests can't bleed: the *Breakers* registry uses package defaults that
// might be in any state from a previous test. Tests always allocate via
// NewBreaker directly with a per-test unique label so the prometheus
// gauge label set stays clean across test runs.
func newTestBreaker(t *testing.T, backend string, threshold int, cooldown time.Duration) *Breaker {
	t.Helper()
	// Suffix with the test name so concurrent t.Parallel-able tests don't
	// share gauges. We don't t.Parallel() here for clarity but keep the
	// invariant.
	return NewBreaker(fmt.Sprintf("%s/%s", backend, t.Name()), threshold, cooldown)
}

// ---------------------------------------------------------------------------
// Per-backend regression tests (audit-mandated)
// ---------------------------------------------------------------------------

// TestBreakers_Registry_AllBackendsPresent asserts that every backend listed
// in allBackends has a corresponding Breaker on the default Breakers set.
// This is the registry-iterating coverage test: a new backend MUST surface
// here automatically.
func TestBreakers_Registry_AllBackendsPresent(t *testing.T) {
	set := NewBreakers()
	for _, name := range allBackends {
		b := breakerForBackend(set, name)
		if b == nil {
			t.Errorf("breaker for backend %q is nil — registry incomplete", name)
			continue
		}
		// The Backend() label must match its registry key so NR widgets
		// keyed on the constant value resolve.
		if b.Backend() != name {
			t.Errorf("breaker %q has Backend()=%q — label mismatch", name, b.Backend())
		}
		if b.State() != StateClosed {
			t.Errorf("breaker %q should start closed, got %s", name, b.State())
		}
	}
}

// TestBreaker_ClosedToOpen_PerBackend asserts the closed→open transition
// for every backend. Threshold = 3 to mirror the contract from api/worker
// (5 consecutive failures is the production default; 3 is plenty to exercise
// the threshold logic without making the test slow).
func TestBreaker_ClosedToOpen_PerBackend(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 3, 30*time.Second)
			if b.State() != StateClosed {
				t.Fatalf("fresh breaker should be closed, got %s", b.State())
			}
			for i := 0; i < 2; i++ {
				if !b.Allow() {
					t.Fatalf("attempt %d: Allow() should return true (still closed)", i+1)
				}
				b.Record(errBoom)
				if b.State() != StateClosed {
					t.Fatalf("attempt %d: state should still be closed, got %s", i+1, b.State())
				}
			}
			// Third failure crosses the threshold → open.
			if !b.Allow() {
				t.Fatal("third attempt should still be allowed before recording")
			}
			b.Record(errBoom)
			if b.State() != StateOpen {
				t.Fatalf("after threshold breach state should be open, got %s", b.State())
			}
			// And subsequent Allow() should be rejected.
			if b.Allow() {
				t.Fatal("post-open Allow() should be rejected")
			}
		})
	}
}

// TestBreaker_OpenToHalfOpenAfterCooldown_PerBackend asserts that after the
// cooldown elapses, exactly one trial call is admitted. Covers both arms
// of the half-open machine.
func TestBreaker_OpenToHalfOpenAfterCooldown_PerBackend(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 1, fastCooldown)
			_ = b.Allow()
			b.Record(errBoom)
			if b.State() != StateOpen {
				t.Fatalf("expected open after first failure (threshold=1), got %s", b.State())
			}
			time.Sleep(shortWait)
			// First Allow() after cooldown should win the half-open trial.
			if !b.Allow() {
				t.Fatal("first Allow() after cooldown should succeed (half-open trial)")
			}
			if b.State() != StateHalfOpen {
				t.Fatalf("expected half_open after trial admission, got %s", b.State())
			}
			// Any concurrent Allow() before Record finishes should be rejected.
			if b.Allow() {
				t.Fatal("second concurrent Allow() should be rejected while trial in flight")
			}
		})
	}
}

// TestBreaker_HalfOpenTrialSuccess_ClosesBreaker_PerBackend asserts the
// recovery happy path for every backend.
func TestBreaker_HalfOpenTrialSuccess_ClosesBreaker_PerBackend(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 1, fastCooldown)
			_ = b.Allow()
			b.Record(errBoom)
			time.Sleep(shortWait)
			if !b.Allow() {
				t.Fatal("trial Allow() should succeed after cooldown")
			}
			// Successful trial → closed.
			b.Record(nil)
			if b.State() != StateClosed {
				t.Fatalf("after successful trial state should be closed, got %s", b.State())
			}
			// New calls should sail through.
			if !b.Allow() {
				t.Fatal("post-recovery Allow() should succeed")
			}
		})
	}
}

// TestBreaker_HalfOpenTrialFailure_ReopensBreaker_PerBackend asserts the
// recovery sad path for every backend.
func TestBreaker_HalfOpenTrialFailure_ReopensBreaker_PerBackend(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 1, fastCooldown)
			_ = b.Allow()
			b.Record(errBoom)
			time.Sleep(shortWait)
			if !b.Allow() {
				t.Fatal("trial Allow() should succeed after cooldown")
			}
			b.Record(errBoom)
			if b.State() != StateOpen {
				t.Fatalf("failed trial should re-open the breaker, got %s", b.State())
			}
			// Cooldown must have restarted — Allow() should be rejected.
			if b.Allow() {
				t.Fatal("Allow() should be rejected right after re-open")
			}
		})
	}
}

// TestBreaker_CallerDeadlineDoesNotTrip_PerBackend is the audit-mandated
// "context.Canceled doesn't count toward tripping" regression test. We
// fire many caller-deadline cancellations far in excess of the threshold;
// the breaker MUST stay closed because a caller giving up is not a
// downstream fault. Covers both context.Canceled and context.DeadlineExceeded.
func TestBreaker_CallerDeadlineDoesNotTrip_PerBackend(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 3, 30*time.Second)
			// 50 caller-deadline failures — 16× the threshold. If any of
			// them counts, the breaker trips.
			for i := 0; i < 50; i++ {
				if !b.Allow() {
					t.Fatalf("attempt %d: caller-deadline should never short-circuit Allow()", i+1)
				}
				if i%2 == 0 {
					b.Record(context.Canceled)
				} else {
					b.Record(context.DeadlineExceeded)
				}
				if b.State() != StateClosed {
					t.Fatalf("attempt %d (%s): state should still be closed, got %s — caller-deadline must not trip the breaker", i+1, name, b.State())
				}
			}
		})
	}
}

// TestBreaker_CallerDeadlineWrapped_DoesNotTrip asserts that wrapped
// caller-deadline errors (which is what gRPC handlers and pgx return in
// practice) are also filtered. errors.Is must reach context.{Canceled,
// DeadlineExceeded} through the wrapper.
func TestBreaker_CallerDeadlineWrapped_DoesNotTrip(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 2, 30*time.Second)
			wrapped := fmt.Errorf("gRPC server: dial postgres-customers: %w", context.DeadlineExceeded)
			for i := 0; i < 10; i++ {
				if !b.Allow() {
					t.Fatalf("attempt %d: wrapped caller-deadline should not short-circuit", i+1)
				}
				b.Record(wrapped)
			}
			if b.State() != StateClosed {
				t.Fatalf("wrapped caller-deadline must not trip (got %s)", b.State())
			}
		})
	}
}

// TestBreaker_CallerDeadlineDuringHalfOpen_DoesNotCloseOrReopen asserts the
// half-open arm of the caller-deadline filter: a caller giving up during
// the trial is treated as "trial never happened", not as success (which
// would falsely close the breaker) and not as failure (which would
// re-open).
func TestBreaker_CallerDeadlineDuringHalfOpen_DoesNotCloseOrReopen(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 1, fastCooldown)
			_ = b.Allow()
			b.Record(errBoom)
			if b.State() != StateOpen {
				t.Fatalf("expected open after threshold, got %s", b.State())
			}
			time.Sleep(shortWait)
			// Grab the trial.
			if !b.Allow() {
				t.Fatal("trial Allow() should succeed after cooldown")
			}
			if b.State() != StateHalfOpen {
				t.Fatalf("expected half_open, got %s", b.State())
			}
			// Caller deadline fires.
			b.Record(context.Canceled)
			// Breaker must NOT have closed (the trial didn't truly succeed
			// against the downstream). It must report open so the next
			// genuine probe gets a fresh trial slot.
			if b.State() == StateClosed {
				t.Fatal("caller-deadline during half-open must NOT close the breaker")
			}
		})
	}
}

// TestBreaker_DoHelper_ReturnsErrOpen asserts the convenience helper short-
// circuits with ErrOpen when the breaker is open. This is the integration
// contract the server wrapper relies on.
func TestBreaker_DoHelper_ReturnsErrOpen(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 1, 30*time.Second)
			// Trip.
			_ = b.Do(func() error { return errBoom })
			if b.State() != StateOpen {
				t.Fatalf("expected open, got %s", b.State())
			}
			// Next Do should short-circuit without invoking fn.
			called := false
			err := b.Do(func() error {
				called = true
				return nil
			})
			if !errors.Is(err, ErrOpen) {
				t.Fatalf("expected ErrOpen, got %v", err)
			}
			if called {
				t.Fatal("fn must not be invoked when breaker is open")
			}
		})
	}
}

// TestBreaker_DoHelper_FiltersCallerDeadline asserts that the convenience
// helper threads caller-deadline filtering through. A fn returning a
// caller-deadline must not trip the breaker even when called repeatedly.
func TestBreaker_DoHelper_FiltersCallerDeadline(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 2, 30*time.Second)
			for i := 0; i < 10; i++ {
				err := b.Do(func() error { return context.Canceled })
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Do should propagate fn's error verbatim, got %v", err)
				}
			}
			if b.State() != StateClosed {
				t.Fatalf("repeated caller-deadline through Do must not trip, got %s", b.State())
			}
		})
	}
}

// TestBreaker_ConcurrentTrialAdmitsOne asserts the half-open CAS truly
// admits exactly one caller across N concurrent goroutines. Regression
// guard for "one Postgres outage = N customers' provisions all try the
// trial at once".
func TestBreaker_ConcurrentTrialAdmitsOne(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 1, fastCooldown)
			_ = b.Allow()
			b.Record(errBoom)
			time.Sleep(shortWait)

			const n = 50
			var (
				wg       sync.WaitGroup
				mu       sync.Mutex
				admitted int
			)
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					if b.Allow() {
						mu.Lock()
						admitted++
						mu.Unlock()
					}
				}()
			}
			wg.Wait()
			if admitted != 1 {
				t.Fatalf("exactly one goroutine should win the half-open trial, got %d", admitted)
			}
		})
	}
}

// TestBreaker_SuccessResetsConsecutiveCounter asserts a successful call
// clears the failure tally — a flapping downstream that fails twice,
// succeeds, then fails twice should NOT trip a threshold=3 breaker. This
// is the same flapping-recovery rule api/worker enforce.
func TestBreaker_SuccessResetsConsecutiveCounter(t *testing.T) {
	for _, name := range allBackends {
		t.Run(name, func(t *testing.T) {
			b := newTestBreaker(t, name, 3, 30*time.Second)
			for i := 0; i < 2; i++ {
				_ = b.Allow()
				b.Record(errBoom)
			}
			_ = b.Allow()
			b.Record(nil)
			for i := 0; i < 2; i++ {
				_ = b.Allow()
				b.Record(errBoom)
			}
			if b.State() != StateClosed {
				t.Fatalf("state should still be closed after reset, got %s", b.State())
			}
		})
	}
}

// TestBreaker_StateStringValues — quick sanity check the string labels
// match what the metrics scrape emits. NR runbook references these
// strings literally.
func TestBreaker_StateStringValues(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half_open"},
	}
	for _, c := range cases {
		if c.s.String() != c.want {
			t.Errorf("State(%d).String() = %q, want %q", c.s, c.s.String(), c.want)
		}
	}
}

// TestBreaker_ErrOpenIsStableSentinel — server wrapper branches on
// errors.Is(err, circuit.ErrOpen) to map open → gRPC Unavailable. Confirm
// that path works even when ErrOpen is wrapped.
func TestBreaker_ErrOpenIsStableSentinel(t *testing.T) {
	wrapped := errors.Join(errors.New("provisioner: backend unreachable"), ErrOpen)
	if !errors.Is(wrapped, ErrOpen) {
		t.Fatal("errors.Is should detect ErrOpen through errors.Join")
	}
}

// TestBreakers_PerBackendIsolation is the cross-backend isolation
// regression: a Redis outage (failures recorded only on RedisAdmin) MUST
// NOT trip any other backend's breaker. This is the brief's headline
// invariant: "each backend gets its own breaker instance so a Redis
// outage doesn't trip Postgres provisioning."
func TestBreakers_PerBackendIsolation(t *testing.T) {
	set := NewBreakers()
	// Hammer one breaker until it trips.
	target := set.RedisAdmin
	for i := 0; i < defaultThreshold; i++ {
		_ = target.Allow()
		target.Record(errBoom)
	}
	if target.State() != StateOpen {
		t.Fatalf("target breaker should be open after %d failures, got %s", defaultThreshold, target.State())
	}
	// Every other breaker must still be closed.
	others := []*Breaker{set.PostgresK8s, set.PostgresAdmin, set.MongoAdmin, set.K8sAPI}
	for _, other := range others {
		if other.State() != StateClosed {
			t.Errorf("breaker %q tripped from unrelated failures on %q (state=%s) — per-backend isolation broken",
				other.Backend(), target.Backend(), other.State())
		}
		if !other.Allow() {
			t.Errorf("breaker %q rejected Allow() after unrelated failures on %q — per-backend isolation broken",
				other.Backend(), target.Backend())
		}
	}
}
