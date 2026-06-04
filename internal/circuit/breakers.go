package circuit

import (
	"log/slog"
	"time"
)

// Backend names exposed via the `backend` label on
// instant_provisioner_circuit_state and the related counters. These string
// constants are the authoritative spelling — NR widgets and alerting rules
// reference these literal values, so any rename has to land in the
// dashboards repo at the same time.
const (
	BackendPostgresK8s   = "postgres_k8s"   // dedicated Postgres k8s pod / NetworkPolicy / PVC operations
	BackendPostgresAdmin = "postgres_admin" // shared postgres-customers CREATE DATABASE / CREATE USER (local.go)
	BackendRedisAdmin    = "redis_admin"    // shared redis-provision ACL SETUSER / namespace ops
	BackendMongoAdmin    = "mongo_admin"    // shared mongo admin CREATE USER / role grants
	BackendQueueAdmin    = "queue_admin"    // shared NATS account/JWT provisioning + deprovision
	BackendK8sAPI        = "k8s_api"        // raw kube-apiserver client calls (kubectl-equivalent)
)

// Default thresholds — match api/worker's `name=provisioner` breaker
// (threshold=5, cooldown=30s). Five consecutive failures is empirically
// enough to ride out a single kube-apiserver burp without false trips, and
// 30s of fast-fail is the largest cooldown that does not noticeably degrade
// recovery time for a Cluster-API restart.
const (
	defaultThreshold = 5
	defaultCooldown  = 30 * time.Second
)

// Breakers groups every per-backend breaker exposed by this package. One
// instance per process is enough — they hold only atomic counters and a
// metric registration handle. The Server wires the corresponding breaker
// into each backend dispatch path.
//
// Each breaker is independent: a Redis outage MUST NOT count as a failure
// against Postgres provisioning. The brief calls this out explicitly.
type Breakers struct {
	PostgresK8s   *Breaker
	PostgresAdmin *Breaker
	RedisAdmin    *Breaker
	MongoAdmin    *Breaker
	QueueAdmin    *Breaker
	K8sAPI        *Breaker
}

// NewBreakers constructs the per-backend breaker set. Threshold + cooldown
// can be overridden per-backend by passing a non-zero value in `overrides`;
// zero values fall through to the package defaults.
//
// onOpen is wired as a generic "breaker opened" slog warning — the
// production wiring uses this to fire a single structured log line that the
// NR alert filter picks up without any per-breaker glue.
func NewBreakers() *Breakers {
	return &Breakers{
		PostgresK8s:   newDefault(BackendPostgresK8s),
		PostgresAdmin: newDefault(BackendPostgresAdmin),
		RedisAdmin:    newDefault(BackendRedisAdmin),
		MongoAdmin:    newDefault(BackendMongoAdmin),
		QueueAdmin:    newDefault(BackendQueueAdmin),
		K8sAPI:        newDefault(BackendK8sAPI),
	}
}

func newDefault(name string) *Breaker {
	return NewBreaker(name, defaultThreshold, defaultCooldown).WithOnOpen(func() {
		// Single, structured, runbook-aware slog line. The NR alert filter
		// keys off `circuit.opened` + backend label. Keep this cheap; do
		// NOT fan out to PagerDuty here.
		slog.Warn("provisioner.circuit.opened",
			"backend", name,
			"remediation", "see provisioner runbook — downstream "+name+" is degrading",
		)
	})
}

// Default is the process-wide breaker set. Lazily constructed once. Tests
// MUST NOT use this — they construct their own Breakers via NewBreakers()
// to keep state local to the test.
var Default = NewBreakers()
