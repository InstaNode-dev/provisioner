package redis

// dispatch_test.go — routing tests for TierDispatchBackend.
//
// These tests use lightweight recording stubs (no fake clientset needed — the
// dispatcher only delegates to two Backend interfaces) to assert that:
//   - Provision routes Team → dedicated, every non-Team tier → shared carve.
//   - Deprovision / StorageBytes route by provider_resource_id prefix.
//   - Regrade forwards only dedicated-PRID resources to the dedicated Regrader,
//     soft-skips shared-carve resources, and soft-skips when the dedicated
//     backend has no Regrader.
//
// The Provision tier table is driven off the live plans registry (rule 18) so a
// new tier added to plans.yaml is automatically exercised — it cannot silently
// route to the wrong backend.

import (
	"context"
	"errors"
	"testing"

	"instant.dev/common/plans"
)

// route is which backend a stub represents, so tests can assert the call landed
// on the expected side without inspecting unexported dispatcher fields.
type route string

const (
	routeShared    route = "shared_carve"
	routeDedicated route = "dedicated"
)

// recordingBackend is a Backend stub that records the last call it received.
// It does NOT implement Regrader (regradeableBackend does) so the
// "dedicated has no Regrader" branch is reachable.
type recordingBackend struct {
	route route

	provisionCalled    bool
	provisionTier      string
	deprovisionCalled  bool
	deprovisionPRID    string
	storageBytesCalled bool
	storageBytesPRID   string
}

func (b *recordingBackend) Provision(_ context.Context, _, tier string) (*Credentials, error) {
	b.provisionCalled = true
	b.provisionTier = tier
	// Stamp the route into ProviderResourceID so a caller can confirm which
	// backend served the provision.
	return &Credentials{ProviderResourceID: string(b.route)}, nil
}

func (b *recordingBackend) Deprovision(_ context.Context, _, providerResourceID string) error {
	b.deprovisionCalled = true
	b.deprovisionPRID = providerResourceID
	return nil
}

func (b *recordingBackend) StorageBytes(_ context.Context, _, providerResourceID string) (int64, error) {
	b.storageBytesCalled = true
	b.storageBytesPRID = providerResourceID
	return 0, nil
}

// regradeableBackend is a recordingBackend that ALSO implements Regrader, so the
// dispatcher's Regrade-forwarding branch can be exercised.
type regradeableBackend struct {
	recordingBackend
	regradeCalled bool
	regradePRID   string
	regradeMB     int
	regradeResult RegradeResult
	regradeErr    error
}

func (b *regradeableBackend) Regrade(_ context.Context, _, providerResourceID string, targetMaxmemoryMB int) (RegradeResult, error) {
	b.regradeCalled = true
	b.regradePRID = providerResourceID
	b.regradeMB = targetMaxmemoryMB
	return b.regradeResult, b.regradeErr
}

// ─── Provision routing ──────────────────────────────────────────────────────

// TestDispatchProvision_RoutesByTier_RegistryDriven iterates every tier in the
// live plans registry and asserts the dispatcher sends Team to the dedicated
// backend and every other tier to the shared-carve backend. Registry-driven so
// a new plans.yaml tier is covered automatically (rule 18).
func TestDispatchProvision_RoutesByTier_RegistryDriven(t *testing.T) {
	reg := plans.Default()
	tiers := reg.All()
	if len(tiers) == 0 {
		t.Fatal("plans registry returned no tiers — cannot validate routing")
	}

	sawTeam := false
	sawNonTeam := false
	for tier := range tiers {
		t.Run(tier, func(t *testing.T) {
			shared := &recordingBackend{route: routeShared}
			dedicated := &recordingBackend{route: routeDedicated}
			d := NewTierDispatchBackend(shared, dedicated)

			creds, err := d.Provision(context.Background(), "tok-"+tier, tier)
			if err != nil {
				t.Fatalf("Provision(%q) error: %v", tier, err)
			}

			wantRoute := routeShared
			if tier == dedicatedTier {
				wantRoute = routeDedicated
			}

			if creds.ProviderResourceID != string(wantRoute) {
				t.Errorf("tier %q routed to %q, want %q", tier, creds.ProviderResourceID, wantRoute)
			}
			if wantRoute == routeDedicated {
				if !dedicated.provisionCalled || shared.provisionCalled {
					t.Errorf("tier %q: dedicated.called=%v shared.called=%v, want dedicated only",
						tier, dedicated.provisionCalled, shared.provisionCalled)
				}
				sawTeam = true
			} else {
				if !shared.provisionCalled || dedicated.provisionCalled {
					t.Errorf("tier %q: shared.called=%v dedicated.called=%v, want shared only",
						tier, shared.provisionCalled, dedicated.provisionCalled)
				}
				sawNonTeam = true
			}
		})
	}

	// Guard against a registry that somehow contains only one class of tier:
	// the test is only meaningful if it exercised BOTH routes.
	if !sawTeam {
		t.Errorf("no tier routed to the dedicated backend — expected %q to exist in the registry", dedicatedTier)
	}
	if !sawNonTeam {
		t.Error("no tier routed to the shared-carve backend — registry must have non-Team tiers")
	}
}

// TestDispatchProvision_UnknownTier_RoutesShared asserts the fail-safe default:
// a tier the dispatcher does not recognise is treated as non-Team (shared
// carve), never silently handed a dedicated pod.
func TestDispatchProvision_UnknownTier_RoutesShared(t *testing.T) {
	shared := &recordingBackend{route: routeShared}
	dedicated := &recordingBackend{route: routeDedicated}
	d := NewTierDispatchBackend(shared, dedicated)

	if _, err := d.Provision(context.Background(), "tok", "some-future-tier"); err != nil {
		t.Fatalf("Provision error: %v", err)
	}
	if !shared.provisionCalled || dedicated.provisionCalled {
		t.Errorf("unknown tier: shared.called=%v dedicated.called=%v, want shared only",
			shared.provisionCalled, dedicated.provisionCalled)
	}
}

// ─── Deprovision routing (by PRID prefix) ───────────────────────────────────

func TestDispatchDeprovision_RoutesByPRID(t *testing.T) {
	cases := []struct {
		name string
		prid string
		want route
	}{
		{"dedicated namespace PRID", dedicatedNamespacePrefix + "abc123", routeDedicated},
		{"empty PRID (live shared carve)", "", routeShared},
		{"pool-token marker PRID", "pooltok:pool-xyz", routeShared},
		{"local cluster PRID", "local:2", routeShared},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shared := &recordingBackend{route: routeShared}
			dedicated := &recordingBackend{route: routeDedicated}
			d := NewTierDispatchBackend(shared, dedicated)

			if err := d.Deprovision(context.Background(), "tok", tc.prid); err != nil {
				t.Fatalf("Deprovision error: %v", err)
			}
			if tc.want == routeDedicated {
				if !dedicated.deprovisionCalled || shared.deprovisionCalled {
					t.Errorf("PRID %q: dedicated=%v shared=%v, want dedicated only",
						tc.prid, dedicated.deprovisionCalled, shared.deprovisionCalled)
				}
			} else {
				if !shared.deprovisionCalled || dedicated.deprovisionCalled {
					t.Errorf("PRID %q: shared=%v dedicated=%v, want shared only",
						tc.prid, shared.deprovisionCalled, dedicated.deprovisionCalled)
				}
			}
		})
	}
}

// ─── StorageBytes routing (by PRID prefix) ──────────────────────────────────

func TestDispatchStorageBytes_RoutesByPRID(t *testing.T) {
	cases := []struct {
		name string
		prid string
		want route
	}{
		{"dedicated namespace PRID", dedicatedNamespacePrefix + "deadbeef", routeDedicated},
		{"empty PRID (live shared carve)", "", routeShared},
		{"pool-token marker PRID", "pooltok:pool-xyz", routeShared},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shared := &recordingBackend{route: routeShared}
			dedicated := &recordingBackend{route: routeDedicated}
			d := NewTierDispatchBackend(shared, dedicated)

			if _, err := d.StorageBytes(context.Background(), "tok", tc.prid); err != nil {
				t.Fatalf("StorageBytes error: %v", err)
			}
			if tc.want == routeDedicated {
				if !dedicated.storageBytesCalled || shared.storageBytesCalled {
					t.Errorf("PRID %q: dedicated=%v shared=%v, want dedicated only",
						tc.prid, dedicated.storageBytesCalled, shared.storageBytesCalled)
				}
			} else {
				if !shared.storageBytesCalled || dedicated.storageBytesCalled {
					t.Errorf("PRID %q: shared=%v dedicated=%v, want shared only",
						tc.prid, shared.storageBytesCalled, dedicated.storageBytesCalled)
				}
			}
		})
	}
}

// ─── Regrade routing ────────────────────────────────────────────────────────

// TestDispatchRegrade_DedicatedPRID_ForwardsToRegrader asserts a dedicated
// (k8s namespace) resource is forwarded to the dedicated backend's Regrader with
// the PRID and target untouched.
func TestDispatchRegrade_DedicatedPRID_ForwardsToRegrader(t *testing.T) {
	shared := &recordingBackend{route: routeShared}
	dedicated := &regradeableBackend{
		recordingBackend: recordingBackend{route: routeDedicated},
		regradeResult:    RegradeResult{Applied: true, AppliedMaxmemory: 512 << 20},
	}
	d := NewTierDispatchBackend(shared, dedicated)

	prid := dedicatedNamespacePrefix + "abc"
	res, err := d.Regrade(context.Background(), "tok", prid, 512)
	if err != nil {
		t.Fatalf("Regrade error: %v", err)
	}
	if !dedicated.regradeCalled {
		t.Fatal("dedicated Regrader was not called for a dedicated PRID")
	}
	if dedicated.regradePRID != prid {
		t.Errorf("forwarded PRID = %q, want %q", dedicated.regradePRID, prid)
	}
	if dedicated.regradeMB != 512 {
		t.Errorf("forwarded targetMaxmemoryMB = %d, want 512", dedicated.regradeMB)
	}
	if !res.Applied {
		t.Errorf("Regrade result Applied = false, want true (dedicated result must pass through)")
	}
}

// TestDispatchRegrade_DedicatedPRID_PropagatesError asserts the dedicated
// Regrader's error is propagated verbatim (so the server can map it).
func TestDispatchRegrade_DedicatedPRID_PropagatesError(t *testing.T) {
	wantErr := errors.New("boom")
	dedicated := &regradeableBackend{
		recordingBackend: recordingBackend{route: routeDedicated},
		regradeErr:       wantErr,
	}
	d := NewTierDispatchBackend(&recordingBackend{route: routeShared}, dedicated)

	_, err := d.Regrade(context.Background(), "tok", dedicatedNamespacePrefix+"x", 50)
	if !errors.Is(err, wantErr) {
		t.Errorf("Regrade error = %v, want %v", err, wantErr)
	}
}

// TestDispatchRegrade_SharedPRID_SoftSkip is the critical safety guard: a
// shared-carve resource must NEVER reach the dedicated Regrader (which would
// CONFIG SET maxmemory on the shared pod, capping every co-tenant). It returns
// a soft skip with no error and no Regrader call.
func TestDispatchRegrade_SharedPRID_SoftSkip(t *testing.T) {
	dedicated := &regradeableBackend{recordingBackend: recordingBackend{route: routeDedicated}}
	d := NewTierDispatchBackend(&recordingBackend{route: routeShared}, dedicated)

	for _, prid := range []string{"", "pooltok:pool-xyz", "local:1"} {
		res, err := d.Regrade(context.Background(), "tok", prid, 50)
		if err != nil {
			t.Fatalf("Regrade(%q) error: %v (want soft skip, no error)", prid, err)
		}
		if res.Applied {
			t.Errorf("Regrade(%q) Applied = true, want soft skip", prid)
		}
		if res.SkipReason == "" {
			t.Errorf("Regrade(%q) SkipReason empty, want a reason", prid)
		}
	}
	if dedicated.regradeCalled {
		t.Fatal("shared-carve PRID reached the dedicated Regrader — would cap the shared pod for all tenants")
	}
}

// TestDispatchRegrade_DedicatedNoRegrader_SoftSkip asserts that when the
// dedicated backend does NOT implement Regrader, Regrade soft-skips gracefully
// (mirrors the server's existing type-assertion behaviour).
func TestDispatchRegrade_DedicatedNoRegrader_SoftSkip(t *testing.T) {
	// recordingBackend does not implement Regrader.
	dedicated := &recordingBackend{route: routeDedicated}
	d := NewTierDispatchBackend(&recordingBackend{route: routeShared}, dedicated)

	res, err := d.Regrade(context.Background(), "tok", dedicatedNamespacePrefix+"x", 50)
	if err != nil {
		t.Fatalf("Regrade error: %v (want soft skip)", err)
	}
	if res.Applied || res.SkipReason == "" {
		t.Errorf("Regrade with no Regrader = %+v, want Applied=false with a SkipReason", res)
	}
}

// ─── Interface conformance ──────────────────────────────────────────────────

// TestDispatchImplementsRegrader documents that the dispatcher is itself a
// Regrader, so the server's `s.redisBackend.(redis.Regrader)` assertion in
// regradeRedis succeeds when tier-aware routing wraps the backend.
func TestDispatchImplementsRegrader(t *testing.T) {
	var b Backend = NewTierDispatchBackend(&recordingBackend{}, &recordingBackend{})
	if _, ok := b.(Regrader); !ok {
		t.Fatal("TierDispatchBackend does not implement redis.Regrader — server regradeRedis assertion would skip it")
	}
}

// TestNewSharedCarveBackend_IsLocalBackend documents that the shared-carve
// constructor returns a LocalBackend (ACL carve on a shared Redis) — the
// non-Team side of tier-aware routing.
func TestNewSharedCarveBackend_IsLocalBackend(t *testing.T) {
	b := NewSharedCarveBackend("", "localhost:6379")
	if _, ok := b.(*LocalBackend); !ok {
		t.Fatalf("NewSharedCarveBackend returned %T, want *LocalBackend", b)
	}
	// A LocalBackend deliberately does NOT implement Regrader (no per-tenant
	// maxmemory lever on a shared pod) — guard that invariant, since the
	// dispatcher's Regrade safety relies on the shared side having no Regrader.
	if _, ok := b.(Regrader); ok {
		t.Error("shared-carve backend implements Regrader — a shared pod has no per-tenant maxmemory to regrade")
	}
}

// TestIsDedicatedRoutingTier documents the exact tier→route predicate.
func TestIsDedicatedRoutingTier(t *testing.T) {
	if !isDedicatedRoutingTier(dedicatedTier) {
		t.Errorf("isDedicatedRoutingTier(%q) = false, want true", dedicatedTier)
	}
	for _, tier := range []string{"anonymous", "free", "hobby", "hobby_plus", "pro", "growth", ""} {
		if isDedicatedRoutingTier(tier) {
			t.Errorf("isDedicatedRoutingTier(%q) = true, want false (non-Team must use shared carve)", tier)
		}
	}
}
