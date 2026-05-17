package postgres

import "testing"

// TestAdminURLForResource_EmptySliceNoPanic — regression for the P2 panic:
// AdminURLForResource used to index r.adminURLs[0] unconditionally, so an
// empty slice crashed every lifecycle RPC. It must fail soft with "" instead.
func TestAdminURLForResource_EmptySliceNoPanic(t *testing.T) {
	r := &ClusterRouter{adminURLs: nil}
	for _, prid := range []string{"", "local:0", "local:5", "neon:whatever"} {
		if got := r.AdminURLForResource(prid); got != "" {
			t.Errorf("AdminURLForResource(%q) on empty router = %q; want \"\"", prid, got)
		}
	}
}

// TestAdminURLForResource_RoutesByIndex — with clusters configured the resource
// still routes by the "local:<N>" provider_resource_id, and out-of-range or
// legacy (empty) values fall back to cluster 0.
func TestAdminURLForResource_RoutesByIndex(t *testing.T) {
	r := newClusterRouter([]string{"url0", "url1", "url2"}, 0)
	cases := []struct {
		prid string
		want string
	}{
		{"", "url0"},          // legacy empty → cluster 0
		{"local:0", "url0"},   // explicit index
		{"local:2", "url2"},   // explicit index
		{"local:9", "url0"},   // out of range → cluster 0
		{"local:-1", "url0"},  // negative → cluster 0
		{"local:x", "url0"},   // unparseable → cluster 0
		{"neon:abc", "url0"},  // not a local resource → cluster 0
	}
	for _, tc := range cases {
		if got := r.AdminURLForResource(tc.prid); got != tc.want {
			t.Errorf("AdminURLForResource(%q) = %q; want %q", tc.prid, got, tc.want)
		}
	}
}

// TestNewLocalBackendMulti_EmptyFallsBackToDefault — an empty adminURLs slice
// would leave the router with no clusters; the constructor must fall back to
// the default customers URL so Pick / AdminURLForResource always have a target.
func TestNewLocalBackendMulti_EmptyFallsBackToDefault(t *testing.T) {
	for _, urls := range [][]string{nil, {}} {
		b := newLocalBackendMulti(urls)
		if got := b.router.AdminURLForResource(""); got != defaultCustomersURL {
			t.Errorf("newLocalBackendMulti(%v): AdminURLForResource(\"\") = %q; want default %q",
				urls, got, defaultCustomersURL)
		}
		if _, url, err := b.router.Pick(); err != nil || url != defaultCustomersURL {
			t.Errorf("newLocalBackendMulti(%v): Pick() = (%q, %v); want default URL, nil", urls, url, err)
		}
	}
}
