package postgres

// cluster_router_live_test.go — live-Postgres coverage for the
// ClusterRouter background-polling path (dbCount + refreshCounts) and the
// remaining pure-function helpers (ProviderResourceID).

import (
	"context"
	"testing"
	"time"
)

func TestClusterRouter_ProviderResourceID(t *testing.T) {
	r := newClusterRouter([]string{"a", "b", "c"}, 0)
	for i, want := range []string{"local:0", "local:1", "local:2"} {
		if got := r.ProviderResourceID(i); got != want {
			t.Errorf("ProviderResourceID(%d) = %q; want %q", i, got, want)
		}
	}
}

func TestClusterRouter_DBCount_ConnectError(t *testing.T) {
	r := newClusterRouter([]string{"postgres://u:p@127.0.0.1:1/d?sslmode=disable&connect_timeout=1"}, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := r.dbCount(ctx, r.adminURLs[0]); err == nil {
		t.Error("dbCount on dead admin URL returned nil; want connect error")
	}
}

func TestClusterRouter_DBCount_LiveCluster(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	r := newClusterRouter([]string{adminDSN}, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := r.dbCount(ctx, adminDSN)
	if err != nil {
		t.Fatalf("dbCount: %v", err)
	}
	if n < 0 {
		t.Errorf("dbCount = %d; want >= 0", n)
	}
}

func TestClusterRouter_RefreshCounts_LiveAndDead(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	// One live cluster + one dead one — refreshCounts must succeed for the
	// live one and preserve the dead one's previous count.
	r := newClusterRouter([]string{adminDSN, "postgres://u:p@127.0.0.1:1/d?sslmode=disable&connect_timeout=1"}, 0)
	// Seed the dead cluster's previous count so the keep-stale branch can be
	// observed.
	r.counts[1] = 42
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.refreshCounts(ctx)
	if r.counts[1] != 42 {
		t.Errorf("dead cluster count was overwritten = %d; want preserved 42", r.counts[1])
	}
}

// TestClusterRouter_RefreshCounts_EmptyURLSkipped — an empty admin URL in the
// router slice is skipped without erroring (defensive branch).
func TestClusterRouter_RefreshCounts_EmptyURLSkipped(t *testing.T) {
	r := newClusterRouter([]string{""}, 0)
	r.refreshCounts(context.Background())
	if r.counts[0] != 0 {
		t.Errorf("empty URL produced non-zero count = %d; want 0", r.counts[0])
	}
}

// TestClusterRouter_Pick_NoClusters covers Pick's "no clusters configured"
// branch — uncovered in baseline because every test that constructed a router
// passed at least one URL.
func TestClusterRouter_Pick_NoClusters(t *testing.T) {
	r := &ClusterRouter{}
	_, _, err := r.Pick()
	if err == nil {
		t.Error("Pick on empty router returned nil err; want 'no clusters configured'")
	}
}

// TestClusterRouter_Pick_AllAtCapacity_StillReturnsBest covers the
// fall-back-to-index-0 branch when every cluster is at-or-above capacity.
func TestClusterRouter_Pick_AllAtCapacity_StillReturnsBest(t *testing.T) {
	r := newClusterRouter([]string{"u0", "u1"}, 1)
	// Saturate both clusters so headroom is 0 for both.
	r.counts[0] = 1
	r.counts[1] = 1
	idx, url, err := r.Pick()
	if err != nil {
		t.Fatalf("Pick at capacity returned err = %v; want nil", err)
	}
	if idx != 0 || url != "u0" {
		t.Errorf("Pick at capacity = (%d,%q); want (0,u0) fallback", idx, url)
	}
}

// TestClusterRouter_Pick_AllEmptyURLs covers the "every URL is empty" branch
// where best stays -1 and the function falls through to index 0.
func TestClusterRouter_Pick_AllEmptyURLs(t *testing.T) {
	r := &ClusterRouter{
		adminURLs: []string{"", ""},
		maxDBs:    []int{400, 400},
		counts:    []int{0, 0},
		inflight:  []int{0, 0},
	}
	idx, _, err := r.Pick()
	if err != nil {
		t.Errorf("Pick(all-empty) err = %v; want nil fallback to index 0", err)
	}
	if idx != 0 {
		t.Errorf("Pick(all-empty) = %d; want 0 fallback", idx)
	}
}
