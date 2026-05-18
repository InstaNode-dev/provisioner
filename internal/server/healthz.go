// Sidecar HTTP handler exposing /healthz so the platform can curl the
// running pod's commit_id without going through the gRPC surface.
//
// The provisioner is otherwise gRPC-only on port 50051. The HTTP sidecar
// binds to a different port (default :8092, see OBSERVABILITY-PLAN-2026-05-12.md
// at the api repo root). The non-collision is asserted at startup in main.go.
//
// Relocated 2026-05-12 from the api repo's provisioner/ scaffold subdir
// (track B2 of the observability rollout). Source-of-truth file paths used
// to live in github.com/InstaNode-dev/api under provisioner/internal/server/;
// this file is the canonical copy now.

package server

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"instant.dev/common/buildinfo"
)

const (
	// serviceName is the value reported in HealthzResponse.Service. Dashboards
	// and alert rules key off this constant, so it must match the api/worker
	// /healthz service strings' naming convention exactly.
	serviceName = "instant-provisioner"

	// statusServing / statusNotReady are the human-readable status strings
	// echoed alongside the boolean `ok` field. A liveness probe consumes `ok`;
	// these strings exist so an operator curling /healthz sees *why*.
	statusServing  = "serving"
	statusNotReady = "not_ready"
)

// Readiness is a boolean gate the process flips to true once the gRPC server
// is accepting connections, and back to false at the start of shutdown. It
// makes /healthz report real serving state instead of an unconditional
// ok:true (BugBash 2026-05-18 P3 — "healthz always ok:true").
//
// The zero value is "not ready" — a freshly constructed Readiness reports
// false until SetReady(true) is called, which matches a process that has not
// yet bound its gRPC listener.
type Readiness struct {
	ready atomic.Bool
}

// SetReady flips the readiness gate. Pass true once the gRPC listener is up,
// false at the start of graceful shutdown. Safe for concurrent use.
func (r *Readiness) SetReady(ok bool) { r.ready.Store(ok) }

// IsReady reports the current readiness state. Safe for concurrent use.
func (r *Readiness) IsReady() bool { return r.ready.Load() }

// HealthzResponse is the JSON body returned by GET /healthz.
//
// Field order matches what the api and worker services return so dashboards
// and curl pipelines can use a single jq filter across all three.
type HealthzResponse struct {
	OK        bool   `json:"ok"`
	Service   string `json:"service"`
	Status    string `json:"status"`
	CommitID  string `json:"commit_id"`
	BuildTime string `json:"build_time"`
	Version   string `json:"version"`
}

// HealthzHandler returns an http.Handler that responds with the build metadata
// JSON. When readiness is nil the handler always reports ok:true / 200 —
// preserving the original liveness-only behaviour for callers that only need
// commit_id. When readiness is non-nil, `ok` reflects readiness.IsReady() and
// the handler returns 503 while not ready, so a k8s readiness probe pointed at
// /healthz pulls a not-yet-serving pod out of the Service endpoints instead of
// the endpoint lying ok:true the instant the HTTP sidecar binds (BugBash
// 2026-05-18 P3).
//
// It never errors internally — the response body is a fixed-shape value with
// no unmarshalable types, and a broken connection cannot be reported anyway.
func HealthzHandler(readiness *Readiness) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ok := true
		if readiness != nil {
			ok = readiness.IsReady()
		}
		status := statusServing
		if !ok {
			status = statusNotReady
		}
		resp := HealthzResponse{
			OK:        ok,
			Service:   serviceName,
			Status:    status,
			CommitID:  buildinfo.GitSHA,
			BuildTime: buildinfo.BuildTime,
			Version:   buildinfo.Version,
		}
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		// json.NewEncoder.Encode never errors on a value of fixed shape with
		// no unmarshalable types — and we'd be unable to write an error
		// response anyway if the connection were broken. Discard.
		_ = json.NewEncoder(w).Encode(resp)
	})
}
