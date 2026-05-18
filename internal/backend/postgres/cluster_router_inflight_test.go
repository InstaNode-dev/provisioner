package postgres

// cluster_router_inflight_test.go — regression test for the BugBash-2026-05-18
// P2 fix: ClusterRouter.Pick routes off a 60s-stale poll count, so a burst of
// concurrent Picks between two polls all saw identical headroom and stampeded
// one cluster. Pick now factors in (and increments) an in-flight count;
// ReleasePick decrements it once the provision settles.

import (
	"sync"
	"testing"
)

// TestPick_InFlightSpreadsLoadAcrossClusters — with zero polled count and no
// ReleasePick calls, a run of N Picks must distribute round-robin-ish across
// the configured clusters rather than all landing on index 0.
func TestPick_InFlightSpreadsLoadAcrossClusters(t *testing.T) {
	r := newClusterRouter([]string{"url0", "url1", "url2"}, 0)

	const picks = 9
	hits := map[int]int{}
	for i := 0; i < picks; i++ {
		idx, _, err := r.Pick()
		if err != nil {
			t.Fatalf("Pick() #%d error: %v", i, err)
		}
		hits[idx]++
	}
	// 9 picks across 3 clusters with the in-flight count rising on each pick
	// must spread evenly — 3 each. The pre-fix code put all 9 on cluster 0.
	for c := 0; c < 3; c++ {
		if hits[c] != 3 {
			t.Errorf("cluster %d got %d picks; want 3 — Pick is not spreading via in-flight count (hits=%v)", c, hits[c], hits)
		}
	}
}

// TestReleasePick_DecrementsInFlight — after a provision settles, ReleasePick
// frees the in-flight slot so that cluster becomes eligible again.
func TestReleasePick_DecrementsInFlight(t *testing.T) {
	r := newClusterRouter([]string{"url0", "url1"}, 0)

	// Pick twice — one slot each. Then release cluster 0; the next Pick must
	// prefer cluster 0 again (it has the most headroom).
	idx0, _, _ := r.Pick()
	idx1, _, _ := r.Pick()
	if idx0 == idx1 {
		t.Fatalf("first two Picks both chose cluster %d; want them spread", idx0)
	}
	r.ReleasePick(idx0)
	got, _, _ := r.Pick()
	if got != idx0 {
		t.Errorf("after ReleasePick(%d), next Pick = %d; want %d (freed slot)", idx0, got, idx0)
	}
}

// TestReleasePick_ClampsAtZero — a double release (or a release after a poll
// already absorbed the provision) must not drive the in-flight count negative.
func TestReleasePick_ClampsAtZero(t *testing.T) {
	r := newClusterRouter([]string{"url0"}, 0)
	r.ReleasePick(0)
	r.ReleasePick(0)
	if r.inflight[0] != 0 {
		t.Errorf("inflight[0] = %d after double release; want 0 (clamped)", r.inflight[0])
	}
	// Out-of-range index is a no-op, not a panic.
	r.ReleasePick(99)
	r.ReleasePick(-1)
}

// TestPick_ConcurrentSafe — Pick + ReleasePick under the race detector must not
// data-race and the in-flight count must net to zero after balanced calls.
func TestPick_ConcurrentSafe(t *testing.T) {
	r := newClusterRouter([]string{"url0", "url1", "url2"}, 0)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx, _, err := r.Pick()
			if err != nil {
				t.Errorf("Pick() error: %v", err)
				return
			}
			r.ReleasePick(idx)
		}()
	}
	wg.Wait()
	for c, n := range r.inflight {
		if n != 0 {
			t.Errorf("inflight[%d] = %d after balanced Pick/ReleasePick; want 0", c, n)
		}
	}
}
