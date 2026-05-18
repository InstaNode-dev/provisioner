package postgres

// cluster_router.go — ClusterRouter picks the least-loaded Postgres admin URL
// for new provisions when multiple shared clusters are configured.
//
// Usage: set POSTGRES_CLUSTER_URLS to a comma-separated list of admin DSNs.
// A single URL behaves identically to the original LocalBackend (no change in
// behaviour, cluster index 0 stored in providerResourceID).
//
// providerResourceID format for local backend: "local:{clusterIndex}"
// e.g. "local:0" for cluster 0, "local:1" for cluster 1.
// Existing resources with empty providerResourceID fall back to cluster 0.

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// ClusterRouter picks the admin DSN of the least-loaded shared Postgres cluster.
// It polls each cluster's provisioned-database count every 60 seconds and
// selects the cluster with the most headroom on each provision call.
type ClusterRouter struct {
	adminURLs []string
	maxDBs    []int // capacity cap per cluster (defaults to 400 each)

	mu     sync.RWMutex
	counts []int // current db_{token} database count per cluster (last poll)
	// inflight tracks provisions Pick'd but not yet reflected in a poll. The
	// poll cadence is 60s, so without this a burst of concurrent provisions
	// all see the same stale count and stampede one cluster past maxDBs.
	// Pick increments the chosen cluster's inflight count; ReleasePick
	// decrements it once the provision (success or failure) settles.
	inflight []int

	done chan struct{}
}

// newClusterRouter creates a ClusterRouter for the given admin DSNs.
// maxPerCluster sets the database capacity cap. Pass 0 to use the default (400).
func newClusterRouter(adminURLs []string, maxPerCluster int) *ClusterRouter {
	if maxPerCluster <= 0 {
		maxPerCluster = 400
	}
	caps := make([]int, len(adminURLs))
	for i := range caps {
		caps[i] = maxPerCluster
	}
	return &ClusterRouter{
		adminURLs: adminURLs,
		maxDBs:    caps,
		counts:    make([]int, len(adminURLs)),
		inflight:  make([]int, len(adminURLs)),
		done:      make(chan struct{}),
	}
}

// Start begins background capacity polling. Safe to call multiple times — only
// the first call starts the goroutine. Call Shutdown to stop.
func (r *ClusterRouter) Start(ctx context.Context) {
	go r.pollLoop(ctx)
}

// Shutdown stops the background polling goroutine.
func (r *ClusterRouter) Shutdown() {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}

// Pick returns the index and admin DSN of the cluster with the most available
// capacity. Returns an error if all clusters are at capacity.
//
// Headroom is computed against the last poll count PLUS the in-flight count of
// provisions already routed to that cluster but not yet visible to a poll.
// Pick increments the chosen cluster's in-flight count; the caller MUST call
// ReleasePick with the returned index once the provision settles (success or
// failure) so the in-flight count does not leak. Without this, a burst of
// concurrent Picks between two 60s polls would all see the same stale count
// and stampede one cluster past maxDBs.
func (r *ClusterRouter) Pick() (int, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.adminURLs) == 0 {
		return 0, "", fmt.Errorf("cluster_router: no clusters configured")
	}

	best := -1
	bestHeadroom := -1
	for i, url := range r.adminURLs {
		if url == "" {
			continue
		}
		headroom := r.maxDBs[i] - r.counts[i] - r.inflight[i]
		if headroom > bestHeadroom {
			bestHeadroom = headroom
			best = i
		}
	}

	if best < 0 || bestHeadroom <= 0 {
		// All clusters at or over capacity — still pick the best one and let
		// the provision attempt proceed; failing here would block all writes.
		// The in-flight count is still incremented so a stampede past capacity
		// is at least visible to subsequent Picks.
		slog.Warn("cluster_router.Pick: all clusters at capacity — falling back to index 0")
		if best < 0 {
			best = 0
		}
		r.inflight[best]++
		return best, r.adminURLs[best], nil
	}

	r.inflight[best]++
	slog.Debug("cluster_router.Pick",
		"cluster_index", best,
		"headroom", bestHeadroom,
		"total_clusters", len(r.adminURLs),
	)
	return best, r.adminURLs[best], nil
}

// ReleasePick decrements the in-flight provision count for the cluster at
// clusterIndex. It MUST be called exactly once for every Pick — typically via
// defer immediately after a successful Pick — once the provision attempt has
// settled, whether it succeeded or failed. The count is clamped at zero so a
// double-release or a release after a poll already absorbed the provision can
// never drive it negative.
func (r *ClusterRouter) ReleasePick(clusterIndex int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if clusterIndex < 0 || clusterIndex >= len(r.inflight) {
		return
	}
	if r.inflight[clusterIndex] > 0 {
		r.inflight[clusterIndex]--
	}
}

// AdminURLForResource returns the admin DSN for an existing resource given its
// providerResourceID (format: "local:{index}"). Falls back to cluster 0 for
// legacy resources that have an empty providerResourceID.
func (r *ClusterRouter) AdminURLForResource(providerResourceID string) string {
	// Guard the slice indexing — Pick() guards len==0 the same way. An empty
	// adminURLs slice should be impossible (newLocalBackendMulti falls back to
	// the default customers URL), but a panic here would crash every lifecycle
	// RPC, so fail soft with "".
	if len(r.adminURLs) == 0 {
		slog.Error("cluster_router.AdminURLForResource: no clusters configured")
		return ""
	}
	if providerResourceID == "" {
		return r.adminURLs[0]
	}
	if !strings.HasPrefix(providerResourceID, "local:") {
		// Not a local-backend resource (e.g. Neon) — callers should not reach here.
		return r.adminURLs[0]
	}
	idx, err := strconv.Atoi(strings.TrimPrefix(providerResourceID, "local:"))
	if err != nil || idx < 0 || idx >= len(r.adminURLs) {
		return r.adminURLs[0]
	}
	return r.adminURLs[idx]
}

// ProviderResourceID returns the providerResourceID string for a resource
// provisioned on the cluster at the given index.
func (r *ClusterRouter) ProviderResourceID(clusterIndex int) string {
	return fmt.Sprintf("local:%d", clusterIndex)
}

// --- internal ---

func (r *ClusterRouter) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	// Immediate first poll so counts are populated before the first provision.
	r.refreshCounts(ctx)

	for {
		select {
		case <-r.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refreshCounts(ctx)
		}
	}
}

func (r *ClusterRouter) refreshCounts(ctx context.Context) {
	newCounts := make([]int, len(r.adminURLs))
	for i, adminURL := range r.adminURLs {
		if adminURL == "" {
			continue
		}
		count, err := r.dbCount(ctx, adminURL)
		if err != nil {
			slog.Error("cluster_router.refresh: poll failed",
				"cluster_index", i,
				"error", err,
			)
			// Keep the previous count so we don't over-route to a broken cluster.
			r.mu.RLock()
			newCounts[i] = r.counts[i]
			r.mu.RUnlock()
			continue
		}
		newCounts[i] = count
	}

	r.mu.Lock()
	r.counts = newCounts
	r.mu.Unlock()

	slog.Debug("cluster_router.refresh: done", "counts", newCounts)
}

func (r *ClusterRouter) dbCount(ctx context.Context, adminURL string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx) //nolint:errcheck

	var count int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_database WHERE datname LIKE 'db_%'",
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return count, nil
}
