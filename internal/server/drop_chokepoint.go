package server

// drop_chokepoint.go — the SINGLE sanctioned wrapper for a customer-data
// destruction (DROP DATABASE / DROP USER / dropDatabase / ACL DELUSER / namespace
// teardown) on the provisioner.
//
// # WHY THIS EXISTS (truehomie-db DROP incident, 2026-06-03)
//
// An active Pro customer's Postgres database + role were dropped on the shared
// postgres-customers cluster by an unidentified path, leaving NO audit_log row.
// The provisioner is a "dumb executor": DeprovisionResource drops whatever
// (token, provider_resource_id, resource_type) it is handed, and — critically —
// it kept no record of its own. So even a drop that *did* go through the gRPC
// funnel produced no provisioner-side trail to attribute it.
//
// guardedDrop closes that gap on the funnel: every backend Deprovision dispatch
// in DeprovisionResource routes through here, which emits a structured DDL-audit
// log line + Prometheus counter BEFORE invoking the backend. This is the
// application-layer analogue of the postgres-customers `log_statement='ddl'` trap
// set during the incident — but always-on, in the provisioner's own log stream,
// and independent of the platform audit_log (which the provisioner cannot write,
// having no platform-DB access).
//
// guardedDrop is the ONLY sanctioned customer-data drop wrapper. The CI guard in
// drop_guard_test.go fails the build if a raw DROP DATABASE / DROP ROLE /
// DROP USER / dropDatabase / ACL DELUSER literal appears in a provisioner backend
// file that is NOT one of the sanctioned deprovision files reached through here —
// so a NEW un-audited drop call site cannot be merged.
//
// SCOPE NOTE: guardedDrop does NOT (yet) refuse a drop based on resource
// terminality / paid-tier — that requires the provisioner to read the platform
// DB (resources.status/tier), a larger change designed + filed in
// docs/ci/DATA-INTEGRITY-DROP-PATH-AUDIT.md (deletion-intent proto field +
// terminality guard, flag-gated). guardedDrop is the always-safe, zero-behaviour-
// change first layer: it makes every sanctioned drop auditable and a new
// un-audited path unmergeable.

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/peer"

	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/provisioner/internal/circuit"
)

// dropTotal counts every customer-data drop the provisioner performs through the
// sanctioned chokepoint, labelled by resource_type, backend (shared|dedicated),
// and outcome (ok|error|breaker_open). A spike in this counter is the alertable
// signal that drops are happening at an abnormal rate (the truehomie incident
// class). Eager-registered (NewCounterVec via promauto on the default registry)
// so the series exists at /metrics before the first drop — but only label
// combinations actually observed appear (standard *Vec behaviour). Rule 25:
// alert + dashboard tile + catalog row ship alongside this metric.
var dropTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "instant_provisioner_drop_total",
	Help: "Customer-data drops performed by the provisioner via the sanctioned chokepoint, by resource_type, backend, and outcome.",
}, []string{"resource_type", "backend", "outcome"})

// dropBackend distinguishes the shared multi-tenant backend from a dedicated
// (per-tenant pod / Neon project) backend in the audit line + metric.
type dropBackend string

const (
	dropBackendShared    dropBackend = "shared"
	dropBackendDedicated dropBackend = "dedicated"
)

// callerFromContext returns a best-effort identifier for the gRPC peer that
// issued the request (e.g. "10.109.3.201:54422"), or "unknown" when the peer is
// not available. This is the provisioner-side attribution that was MISSING in the
// truehomie incident — every sanctioned drop now records who asked for it.
func callerFromContext(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

// guardedDrop is the single sanctioned wrapper for a customer-data destruction.
// It emits the DDL-audit log line + metric, then invokes fn (the backend
// Deprovision) inside the supplied circuit breaker. Every Deprovision dispatch in
// DeprovisionResource MUST call this rather than callBackendVoid directly.
//
// The audit line is emitted BEFORE the drop so a backend that hangs or crashes
// the process mid-drop still leaves the "we are about to drop X" record — exactly
// the trail that was absent in the incident.
func (s *Server) guardedDrop(
	ctx context.Context,
	req *provisionerv1.DeprovisionRequest,
	backend dropBackend,
	breaker *circuit.Breaker,
	fn func() error,
) error {
	resType := req.ResourceType.String()
	caller := callerFromContext(ctx)

	// DDL-audit: the always-on, provisioner-side record of the drop. This is the
	// in-app equivalent of the cluster's log_statement='ddl' trap.
	slog.Info("provisioner.drop",
		"event", "provisioner.drop",
		"token", req.Token,
		"provider_resource_id", req.ProviderResourceId,
		"resource_type", resType,
		"backend", string(backend),
		"request_id", req.RequestId,
		"caller", caller,
	)

	err := callBackendVoid(breaker, fn)

	outcome := dropOutcome(err)
	dropTotal.WithLabelValues(resType, string(backend), outcome).Inc()

	if err != nil {
		slog.Warn("provisioner.drop.failed",
			"event", "provisioner.drop.failed",
			"token", req.Token,
			"provider_resource_id", req.ProviderResourceId,
			"resource_type", resType,
			"backend", string(backend),
			"request_id", req.RequestId,
			"caller", caller,
			"outcome", outcome,
			"error", err,
		)
	}
	return err
}

// dropOutcome maps a backend deprovision error to the metric outcome label.
func dropOutcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case err == circuit.ErrOpen:
		return "breaker_open"
	default:
		return "error"
	}
}
