package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHealthzHandler_ResponseShape pins the JSON contract since dashboards
// and alert rules consume this body shape.
func TestHealthzHandler_ResponseShape(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	HealthzHandler(nil).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var raw map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, key := range []string{"ok", "service", "status", "commit_id", "build_time", "version"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("response missing key %q — keys present: %v", key, mapKeys(raw))
		}
	}

	if raw["service"] != "instant-provisioner" {
		t.Errorf("service = %v, want instant-provisioner", raw["service"])
	}
	if raw["ok"] != true {
		t.Errorf("ok = %v, want true", raw["ok"])
	}
}

// TestHealthzHandler_AcceptsAnyMethod confirms HEAD / POST don't 405. The k8s
// liveness probe sends GET but having the endpoint be method-agnostic makes
// it easier to curl from a shell during incidents.
func TestHealthzHandler_AcceptsAnyMethod(t *testing.T) {
	for _, m := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(m, "/healthz", nil)
		HealthzHandler(nil).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("method %s: status = %d, want 200", m, rec.Code)
		}
	}
}

// TestHealthzHandler_NilReadiness_AlwaysOK pins the liveness-only fallback:
// a nil readiness gate must keep the original unconditional ok:true / 200
// behaviour for callers that only need commit_id.
func TestHealthzHandler_NilReadiness_AlwaysOK(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthzHandler(nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("nil readiness: status = %d, want 200", rec.Code)
	}
	var resp HealthzResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.OK || resp.Status != statusServing {
		t.Errorf("nil readiness: ok=%v status=%q, want true/%q", resp.OK, resp.Status, statusServing)
	}
}

// TestHealthzHandler_ReadinessGate is the regression test for the BugBash
// 2026-05-18 P3 "healthz always ok:true" finding: a not-ready gate must make
// /healthz report ok:false with HTTP 503, and a ready gate must report
// ok:true with 200. A future change that reverts to an unconditional ok:true
// fails the not-ready leg.
func TestHealthzHandler_ReadinessGate(t *testing.T) {
	cases := []struct {
		name       string
		ready      bool
		wantStatus int
		wantOK     bool
		wantBody   string
	}{
		{"not ready → 503", false, http.StatusServiceUnavailable, false, statusNotReady},
		{"ready → 200", true, http.StatusOK, true, statusServing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &Readiness{}
			r.SetReady(tc.ready)
			if got := r.IsReady(); got != tc.ready {
				t.Fatalf("IsReady() = %v, want %v", got, tc.ready)
			}

			rec := httptest.NewRecorder()
			HealthzHandler(r).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			var resp HealthzResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.OK != tc.wantOK {
				t.Errorf("ok = %v, want %v", resp.OK, tc.wantOK)
			}
			if resp.Status != tc.wantBody {
				t.Errorf("status field = %q, want %q", resp.Status, tc.wantBody)
			}
		})
	}
}

// TestReadiness_ZeroValueNotReady pins that a freshly constructed Readiness
// reports false — a process that has not yet bound its gRPC listener.
func TestReadiness_ZeroValueNotReady(t *testing.T) {
	var r Readiness
	if r.IsReady() {
		t.Error("zero-value Readiness reports ready; want not-ready")
	}
}

func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
