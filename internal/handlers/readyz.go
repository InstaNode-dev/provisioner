// Package handlers — HTTP sidecar handlers for the provisioner service.
//
// The provisioner is otherwise gRPC-only. The HTTP sidecar on :8092
// exposes /healthz already (process up + readiness gate from
// internal/server/healthz.go); this package adds /readyz with deep,
// component-by-component checks that the k8s readinessProbe consumes.
//
// /healthz is the livenessProbe target — keep it shallow. /readyz is
// the readinessProbe target — degraded pods stay in the Service
// endpoints list (overall=degraded, 200), failed-critical pods get
// pulled (overall=failed, 503). See common/readiness for the
// criticality rules.
package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"instant.dev/common/readiness"
)

// readyzCheckStatusGauge is the per-service Prometheus gauge — same
// metric name and label set as the api/worker repos so the NR alert
// can query across all three with a single rule.
var readyzCheckStatusGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "readyz_check_status",
	Help: "Per-component readiness status (1=ok, 0.5=degraded, 0=failed). Set by /readyz on every probe.",
}, []string{"service", "check"})

// CircuitInspector is the minimal interface the provisioner's backend
// circuit-breakers must satisfy so /readyz can surface per-backend
// reachability without re-implementing the probe. Tests inject a fake
// for the bad-backend path; production wires the existing
// circuit.Breaker instances kept by internal/server/server.go.
//
// IsOpen() reports whether the breaker is currently tripped. A tripped
// breaker means the backend is failing fast — we surface that as
// degraded (not failed) because the breaker's purpose is exactly to
// keep the provisioner up while one backend is sick; pulling the
// provisioner pod out of rotation would defeat that.
type CircuitInspector interface {
	Name() string
	IsOpen() bool
}

// PoolPinger is the minimal interface for the platform_db check. The
// provisioner's main.go wires this to a lazy adapter that returns
// errPoolNotReady until the pgxpool is set, then forwards Ping to the
// pool. Decoupling lets the HTTP sidecar bind BEFORE the pool exists
// (so /healthz stays up during pool boot) and have /readyz transition
// from failed → ok as soon as main.box.Set runs.
type PoolPinger interface {
	Ping(ctx context.Context) error
}

// ReadyzHandler bundles the provisioner's readiness dependencies.
type ReadyzHandler struct {
	runner *readiness.Runner
	pool   PoolPinger
	// circuits is the set of backend circuit breakers to surface. Empty
	// is fine — the only required check is platform_db. Each breaker
	// appears as a check named "backend_<breaker.Name()>".
	circuits []CircuitInspector
}

// Config tunes the handler's behavior.
type Config struct {
	// PoolPinger is the lazy adapter around the platform/pool DB
	// connection. When non-nil, the platform_db check pings it on
	// every probe (with a 2s timeout, reported as failed-critical on
	// nil-pool / ping-error). When nil, platform_db is NOT registered
	// as a check — operator's intentional choice when the hot-pool
	// feature is disabled (no PROVISIONER_DATABASE_URL). The
	// provisioner serves gRPC fine without the pool, so its /readyz
	// must not 503 the pod out of the Service endpoints over a
	// missing optional dependency. See BugBash B14-P0-F2.
	//
	// Production callers wire the lazy poolProbe adapter when
	// PROVISIONER_DATABASE_URL + AES_KEY are set, and pass nil
	// otherwise.
	PoolPinger PoolPinger
	// Circuits are the per-backend breakers (postgres / redis / mongo
	// / storage / queue). Each one surfaces as a non-critical
	// "backend_<name>" check.
	Circuits []CircuitInspector
}

// NewReadyzHandler wires the runner. Construct once at boot and mount
// h.Get onto the HTTP sidecar's mux at /readyz.
func NewReadyzHandler(cfg Config) *ReadyzHandler {
	h := &ReadyzHandler{
		pool:     cfg.PoolPinger,
		circuits: cfg.Circuits,
	}
	h.runner = readiness.NewRunner(readiness.Config{
		Service: "instant-provisioner",
		// 2s cache window — much shorter than api/worker because the
		// provisioner does not call upstream HTTP APIs (no Brevo /
		// Razorpay), so the cost-of-probing argument doesn't apply.
		// 2s keeps /readyz hot enough that the failed→ok transition
		// after pool init is fast.
		CacheTTL:       2 * time.Second,
		OverallTimeout: 3 * time.Second,
		Metrics:        readyzMetrics{},
	}, h.buildChecks())
	return h
}

// buildChecks registers:
//   - platform_db (CRITICAL): the provisioner's own pgxpool. If the
//     hot-pool DB is unreachable while the pool is enabled, the
//     provisioner can't read its own state — pull from rotation.
//     OMITTED entirely when h.pool == nil — the hot-pool feature is
//     intentionally disabled (no PROVISIONER_DATABASE_URL) and gRPC
//     serving doesn't need it. Registering a failed-critical check
//     in that mode 503s the pod out of the Service endpoints and
//     blocks Prometheus scraping every gauge in the process (the NR
//     alert pipeline blackouts), which is the exact failure mode
//     BugBash B14-P0-F2 surfaced in prod.
//   - backend_<name> (non-critical): one per circuit breaker. A
//     tripped breaker surfaces as degraded. The breaker has its own
//     half-open recovery so /readyz only mirrors it.
func (h *ReadyzHandler) buildChecks() []readiness.Check {
	checks := []readiness.Check{}
	if h.pool != nil {
		checks = append(checks, readiness.Check{
			Name:     "platform_db",
			Critical: true,
			Fn:       h.platformDBCheck(),
		})
	}
	for _, c := range h.circuits {
		c := c // capture
		checks = append(checks, readiness.Check{
			Name:     "backend_" + c.Name(),
			Critical: false,
			Fn: func(ctx context.Context) readiness.CheckResult {
				if c.IsOpen() {
					return readiness.CheckResult{
						Status:    readiness.StatusDegraded,
						LastError: "circuit_open",
					}
				}
				return readiness.CheckResult{Status: readiness.StatusOK}
			},
		})
	}
	return checks
}

// platformDBCheck pings the pgxpool with a 2s timeout. Returns failed
// when the pool is nil (no PROVISIONER_DATABASE_URL configured) OR when
// the lazy adapter reports "not_ready" — both are spotted by the
// operator from the wire output.
func (h *ReadyzHandler) platformDBCheck() readiness.CheckFunc {
	return func(ctx context.Context) readiness.CheckResult {
		if h.pool == nil {
			return readiness.CheckResult{
				Status:    readiness.StatusFailed,
				LastError: "pgxpool_not_configured",
			}
		}
		callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := h.pool.Ping(callCtx); err != nil {
			msg := err.Error()
			if len(msg) > 80 {
				msg = msg[:80]
			}
			return readiness.CheckResult{
				Status:    readiness.StatusFailed,
				LastError: msg,
			}
		}
		return readiness.CheckResult{Status: readiness.StatusOK}
	}
}

// Get is the net/http handler. Mount at /readyz on the sidecar mux.
func (h *ReadyzHandler) Get(w http.ResponseWriter, r *http.Request) {
	resp, code := h.runner.Run(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// readyzMetrics is the Prometheus hook. Stamps service="instant-
// provisioner" — same gauge name as api/worker so dashboards join
// across all three.
type readyzMetrics struct{}

func (readyzMetrics) Observe(name string, status readiness.Status) {
	readyzCheckStatusGauge.WithLabelValues("instant-provisioner", name).Set(statusToFloat(status))
}

func statusToFloat(s readiness.Status) float64 {
	switch s {
	case readiness.StatusOK:
		return 1
	case readiness.StatusDegraded:
		return 0.5
	default:
		return 0
	}
}
