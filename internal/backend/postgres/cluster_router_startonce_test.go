package postgres

// cluster_router_startonce_test.go — regression test for the BugBash-2026-05-18
// P3 fix: ClusterRouter.Start's doc comment promised "Safe to call multiple
// times — only the first call starts the goroutine", but Start unconditionally
// did `go r.pollLoop(ctx)`. A duplicate Start spawned a second poller that
// doubled the DB poll load and leaked on Shutdown (Shutdown closes `done`
// once; the second pollLoop would only exit via ctx cancellation). Start now
// guards pollLoop with sync.Once.

import (
	"context"
	"testing"
	"time"
)

// waitPollStarts polls r.pollStarts until it reaches want or the deadline,
// so the test does not race the goroutine scheduler.
func waitPollStarts(r *ClusterRouter, want int32) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.pollStarts.Load() == want {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// TestStart_OnceGuard_SinglePoller — calling Start many times must spawn
// exactly one poller goroutine. A pre-fix Start spawned N.
func TestStart_OnceGuard_SinglePoller(t *testing.T) {
	// Cancelled context: pollLoop's immediate refreshCounts fails fast against
	// the bogus URL and the loop then exits on ctx.Done() — no DB needed.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := newClusterRouter([]string{"postgres://bogus@127.0.0.1:1/none"}, 0)
	// Join the poller on exit so it cannot outlive this test and call the real
	// pgxConnect after a later test installs a fake seam (Shutdown now Waits).
	defer r.Shutdown()

	for i := 0; i < 5; i++ {
		r.Start(ctx)
	}

	if !waitPollStarts(r, 1) {
		t.Fatalf("pollStarts = %d after 5×Start; want exactly 1 (sync.Once must guard pollLoop)", r.pollStarts.Load())
	}

	// A further Start after the first poller has already exited must still not
	// spawn a new one — sync.Once is permanent.
	r.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	if got := r.pollStarts.Load(); got != 1 {
		t.Errorf("pollStarts = %d after a 6th Start; want 1", got)
	}
}

// TestShutdown_Idempotent — Shutdown must be safe to call more than once;
// it closes `done` under a select guard so a double Shutdown cannot panic on
// a double channel close.
func TestShutdown_Idempotent(t *testing.T) {
	r := newClusterRouter([]string{"url0"}, 0)
	r.Shutdown()
	r.Shutdown() // must not panic on a re-close
}
