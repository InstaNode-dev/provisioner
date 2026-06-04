package pool

// metrics.go — Prometheus instrumentation for the hot-pool reaper (sweep #8).
//
// The Manager has no access to an internal/metrics package (it predates one),
// and main's pool_metrics.go only covers the pgxpool connection stats. These
// counters/gauges are declared here in package pool via promauto so they
// register on the default registry and are picked up by the existing
// /metrics endpoint (main.go: mux.Handle("/metrics", promhttp.Handler())).
//
// Rule 25: every new metric ships with its alert + dashboard tile + catalog
// row in the same PR — see infra/k8s/prometheus-rules.yaml,
// infra/newrelic/{alerts,dashboards}/, infra/observability/METRICS-CATALOG.md.

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// poolReapTotal counts pool_items the reaper has acted on, labelled by the
	// status the row was in (failed) and the outcome of the reap:
	//   outcome="reaped"          — backing infra deprovisioned + row deleted
	//   outcome="deprovision_err" — Deprovision failed; row left for next tick
	//   outcome="delete_err"      — Deprovision ok but row DELETE failed
	// A steady non-zero deprovision_err/delete_err rate means the reaper is
	// wedged and stale infra is accumulating — alertable.
	poolReapTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_pool_reap_total",
		Help: "Hot-pool reaper actions on pool_items, by prior status and outcome.",
	}, []string{"resource_type", "status", "outcome"})

	// poolStuckAssignedGauge reports how many pool_items are stuck in 'assigned'
	// past the stuck-assigned grace. The provisioner CANNOT safely deprovision
	// these: from its own DB an orphaned (crashed-claim) assigned row is
	// indistinguishable from one a live api request successfully bound to a
	// resources row, and deprovisioning a bound item destroys live customer
	// infra (the truehomie-db DROP incident class). So this is surfaced as an
	// operator signal, not auto-reaped. A persistently rising value means the
	// claim path is leaking and needs a resources-table anti-join reaper in a
	// service that owns BOTH pool_items and resources (see PR description).
	poolStuckAssignedGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pool_stuck_assigned",
		Help: "pool_items stuck in 'assigned' past the stuck-assigned grace (operator signal; not auto-reaped — see sweep #8).",
	}, []string{"resource_type"})
)
