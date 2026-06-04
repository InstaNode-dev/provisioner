package circuit

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestQueueBreaker_EmitsCircuitStateMetric — sweep #2 observability guarantee.
//
// The provisioner-circuit-open alert (NR: `FACET backend`; Prom:
// `max(instant_provisioner_circuit_state) by (backend) == 2`) only pages for the
// queue backend if the queue breaker actually EMITS
// instant_provisioner_circuit_state{backend="queue_admin"} — an alert with no
// matching series never fires. This pins that the new QueueAdmin breaker is
// observable end-to-end: tripping it drives the gauge to OPEN and increments the
// per-backend opens counter under the queue_admin label, and (via Record→onOpen)
// emits the circuit.opened / provisioner.circuit.opened log lines the NR alert
// filter also keys on. A regression that stops the queue breaker from emitting
// the metric (or relabels it) reds here before the alert silently goes blind.
func TestQueueBreaker_EmitsCircuitStateMetric(t *testing.T) {
	b := NewBreaker(BackendQueueAdmin, defaultThreshold, defaultCooldown)
	opensBefore := testutil.ToFloat64(breakerOpens.WithLabelValues(BackendQueueAdmin))

	boom := errors.New("nats unreachable: connection refused")
	for i := 0; i < defaultThreshold; i++ {
		b.Record(boom)
	}

	if b.State() != StateOpen {
		t.Fatalf("queue breaker should be OPEN after %d failures, got %v", defaultThreshold, b.State())
	}
	if got := testutil.ToFloat64(breakerState.WithLabelValues(BackendQueueAdmin)); got != float64(StateOpen) {
		t.Errorf("instant_provisioner_circuit_state{backend=%q} = %v, want %v (OPEN) — the circuit-open alert FACETs on this label",
			BackendQueueAdmin, got, float64(StateOpen))
	}
	if got := testutil.ToFloat64(breakerOpens.WithLabelValues(BackendQueueAdmin)); got <= opensBefore {
		t.Errorf("instant_provisioner_circuit_opens_total{backend=%q} did not increment (before=%v after=%v)",
			BackendQueueAdmin, opensBefore, got)
	}
}
