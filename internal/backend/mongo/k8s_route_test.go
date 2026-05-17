package mongo

// k8s_route_test.go — P1-A regression guard for the mongo-proxy route-registry
// key TTL. See redis/k8s_test.go for the sibling redis guard.

import (
	"testing"
	"time"
)

// TestRouteKeyTTLForTier_PaidTiersNeverExpire is the P1-A regression guard.
//
// CONTRACT: route-registry keys for paid/permanent resources MUST be written
// with no expiry (persistRouteKey == 0). The provisioner only re-Sets these
// keys on Provision; a long-lived paid Mongo that is never re-provisioned would
// silently lose its proxy route — and become unreachable — if it carried the
// 365-day TTL.
//
// Anonymous resources (24h lifetime) keep a long self-healing TTL so an
// orphaned key from a failed Deprovision eventually disappears.
//
// If a future change reintroduces a TTL for any paid tier, this test fails.
func TestRouteKeyTTLForTier_PaidTiersNeverExpire(t *testing.T) {
	if persistRouteKey != 0 {
		t.Fatalf("persistRouteKey must be 0 (go-redis: no expiry); got %v", persistRouteKey)
	}
	cases := []struct {
		tier    string
		wantTTL time.Duration
	}{
		{"anonymous", anonRouteKeyTTL}, // only anonymous gets a TTL
		{"hobby", persistRouteKey},
		{"hobby_plus", persistRouteKey},
		{"pro", persistRouteKey},
		{"growth", persistRouteKey},
		{"team", persistRouteKey},
		{"", persistRouteKey},        // empty/unknown → fail safe to persistent
		{"some_future_tier", persistRouteKey},
	}
	for _, tc := range cases {
		t.Run(tc.tier, func(t *testing.T) {
			got := routeKeyTTLForTier(tc.tier)
			if got != tc.wantTTL {
				t.Errorf("routeKeyTTLForTier(%q) = %v; want %v", tc.tier, got, tc.wantTTL)
			}
			if tc.tier != anonymousTier && got != 0 {
				t.Errorf("P1-A REGRESSION: routeKeyTTLForTier(%q) = %v; paid/permanent "+
					"resources must have NO route-key expiry (got non-zero TTL)", tc.tier, got)
			}
		})
	}
}
