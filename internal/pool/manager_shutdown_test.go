package pool

// manager_shutdown_test.go — regression test for the BugBash-2026-05-18 P3 fix:
// the pool Manager's background work ran under the context passed to Start
// (in production, context.Background()), so Shutdown — which only closed the
// `done` channel — left an in-flight provisionOneItem running to its own 60s
// timeout against a process already tearing down. Manager now owns an
// internal runCtx that Shutdown cancels.

import (
	"context"
	"testing"
	"time"
)

// newTestManager builds a Manager without touching a database — enough to
// exercise the Shutdown context-cancellation contract in isolation.
func newTestManager() *Manager {
	return &Manager{
		targets:  map[string]int{},
		refillCh: make(chan string, 1),
		done:     make(chan struct{}),
	}
}

// TestShutdown_BeforeStart_NoPanic — Shutdown must be safe even if Start was
// never called, i.e. runCancel is still nil (e.g. the pool is disabled and
// New ran but Start did not).
func TestShutdown_BeforeStart_NoPanic(t *testing.T) {
	m := newTestManager()
	m.Shutdown() // runCancel == nil — must not panic on the cancel step
}

// TestShutdown_CancelsRunCtx — once Start has wired runCtx/runCancel, Shutdown
// must cancel runCtx so any in-flight provisionOneItem (whose 60s timeout is
// derived from runCtx) aborts promptly instead of running to completion.
func TestShutdown_CancelsRunCtx(t *testing.T) {
	m := newTestManager()
	// Mirror what Start does for the context wiring (without migrate/run,
	// which need a live DB).
	m.runCtx, m.runCancel = context.WithCancel(context.Background())

	select {
	case <-m.runCtx.Done():
		t.Fatal("runCtx already cancelled before Shutdown")
	default:
	}

	// Drain done in a goroutine so Shutdown's wg.Wait (wg is zero here) and
	// close(done) complete; with no run goroutine wg.Wait returns immediately.
	m.Shutdown()

	select {
	case <-m.runCtx.Done():
		// expected — Shutdown cancelled it
	case <-time.After(time.Second):
		t.Fatal("runCtx not cancelled after Shutdown — in-flight provisions would not abort")
	}
	if err := m.runCtx.Err(); err != context.Canceled {
		t.Errorf("runCtx.Err() = %v; want context.Canceled", err)
	}
}
