// Package circuit provides a small, allocation-free circuit breaker primitive
// scoped to the provisioner process. It is a structural copy of api's
// internal/circuit and worker's internal/circuit, so on-call only learns one
// state machine. The shape is intentionally identical; only the metric prefix
// differs (instant_provisioner_circuit_* here, instant_circuit_breaker_* in
// api/worker) so the audit findings can attribute trips to the correct
// process.
//
// # Why we need provisioner-side breakers (audit P0-3)
//
// The provisioner used to rely entirely on the api-side caller breaker. That
// missed a real failure mode: a slow downstream (kube-apiserver, shared
// postgres-customers admin, shared redis admin, mongo admin) makes one gRPC
// call stack up against the 4–5 min provisioning deadline the api caller
// holds. The api breaker counts that as ONE failure — it can't tell apart
// "one stuck call" from "fast clean failure" — so the breaker does not flip
// open until N consecutive deadlines elapse, which is several minutes of
// user-blocking pain per provision.
//
// Putting a breaker IN the provisioner, scoped per-backend, lets the slow
// downstream trip the breaker locally on the first few consecutive failures,
// fast-fail the next callers with Unavailable, and stop the api caller's
// budget from being eaten alive.
//
// # State machine
//
//	closed → (consecutive failures ≥ threshold) → open
//	open   → (cooldown elapsed)                 → half-open (one trial allowed)
//	half-open → (trial succeeds)                → closed
//	half-open → (trial fails)                   → open (cooldown restarts)
//
// All transitions are observable via the instant_provisioner_circuit_state
// gauge (0=closed, 1=open, 2=half_open) labelled by `backend`, plus a
// per-backend `instant_provisioner_circuit_opens_total` counter that drives
// the NR alert "provisioner backend X opened ≥ 3 times in 10 min".
//
// # Caller-deadline filter (audit P1-1 same fix class)
//
// Record() filters context.Canceled and context.DeadlineExceeded from the
// failure record. A caller giving up — the api's per-tier 4–5 min budget
// firing or the api process receiving SIGTERM mid-provision — is not a
// provisioner fault; counting it would inflate the consecutive failure
// counter and trip the breaker on a perfectly healthy downstream.
//
// # Concurrency
//
// All state is held in atomic primitives so Allow / Record can be called
// from any number of goroutines without taking a lock.
package circuit

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// State enumerates the breaker's three possible states. Exported so tests
// and metrics consumers can compare with sentinel values.
type State int32

const (
	// StateClosed — every call is permitted; failures accumulate.
	StateClosed State = 0
	// StateOpen — calls are short-circuited until openUntil elapses.
	StateOpen State = 1
	// StateHalfOpen — exactly one trial call is permitted; success closes
	// the breaker, failure re-opens it.
	StateHalfOpen State = 2
)

// String returns the lowercased label used in NR / Prometheus metrics
// ("closed" | "open" | "half_open"). Matches api/worker.
func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// ErrOpen is the sentinel error returned by callers when the breaker is
// open. The server-side wrapper translates this into a gRPC Unavailable
// status so the api can react cleanly (and its caller-side breaker can
// register the failure honestly).
var ErrOpen = errors.New("provisioner_circuit_breaker_open")

var (
	// breakerOpens counts open transitions (closed→open or half_open→open).
	// Drives the NR alert "provisioner backend X opened ≥ 3 times in 10 min".
	// Label is `backend` to match the brief's metric shape:
	// instant_provisioner_circuit_state{backend=...}.
	breakerOpens = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_provisioner_circuit_opens_total",
		Help: "Provisioner circuit breaker open transitions (closed→open or half_open→open), per backend",
	}, []string{"backend"})

	// breakerAttempts counts every Allow() call regardless of outcome.
	breakerAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_provisioner_circuit_attempts_total",
		Help: "Provisioner circuit breaker Allow() invocations per backend",
	}, []string{"backend"})

	// breakerFailures counts Record(err) calls where err != nil AND err is
	// not a caller-deadline error. (Caller-deadline errors are filtered out
	// inside Record itself — see the doc comment there.)
	breakerFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_provisioner_circuit_failures_total",
		Help: "Provisioner circuit breaker recorded failures (caller-deadline cancellations excluded), per backend",
	}, []string{"backend"})

	// breakerState is sampled on every state transition so an NR widget can
	// show "is the provisioner.postgres_admin breaker currently open?".
	// 0=closed, 1=open, 2=half_open. The brief calls for the metric name
	// `instant_provisioner_circuit_state{backend}` — this is that metric.
	breakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_provisioner_circuit_state",
		Help: "Provisioner circuit breaker state (0=closed, 1=open, 2=half_open), per backend",
	}, []string{"backend"})
)

// Breaker is a single-instance circuit breaker. It is NOT safe to copy
// after first use — all atomic fields rely on a stable memory address.
type Breaker struct {
	backend   string
	threshold int32         // consecutive failures required to open
	cooldown  time.Duration // how long to stay open before allowing one trial

	consecutive atomic.Int32 // current consecutive-failure count; reset on success
	openUntil   atomic.Int64 // UnixNano at which open ends; zero when closed
	halfOpen    atomic.Bool  // true when a half-open trial is in flight

	onOpen func() // optional callback fired on every closed/half_open → open
}

// NewBreaker constructs a Breaker that opens after `threshold` consecutive
// failures and stays open for `cooldown` before allowing a single trial.
//
// threshold MUST be ≥ 1. cooldown MUST be > 0. Misconfigured values are
// snapped to safe defaults rather than rejected — the breaker is on the
// hot path of every provision and a panic on a bad constant would be a
// disproportionate failure mode.
//
// `backend` is used as the only metric label and SHOULD be a short
// snake_case identifier (`postgres_k8s`, `postgres_admin`, `redis_admin`,
// `mongo_admin`, `k8s_api`). Avoid colons / slashes — they're legal
// Prometheus but hurt readability in NR widget titles.
func NewBreaker(backend string, threshold int, cooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	b := &Breaker{
		backend:   backend,
		threshold: int32(threshold),
		cooldown:  cooldown,
	}
	// Seed the state gauge so a freshly-constructed breaker is observable in
	// NR before its first call. Matches api/worker semantics.
	breakerState.WithLabelValues(backend).Set(0)
	return b
}

// WithOnOpen returns the breaker for chaining and installs an optional
// callback fired on every transition into the open state. The provisioner
// wrapper uses this to emit a structured slog event so on-call sees the
// open before the NR 10-min alert window fires.
//
// The callback runs synchronously inside Record(); keep it cheap (slog,
// metric increment).
func (b *Breaker) WithOnOpen(fn func()) *Breaker {
	b.onOpen = fn
	return b
}

// Allow reports whether a call should be attempted right now. See the
// package doc for the state machine. Callers that get `false` MUST NOT
// call Record() — they didn't make the request, so they can't fail it.
// The canonical pattern is to return ErrOpen.
func (b *Breaker) Allow() bool {
	breakerAttempts.WithLabelValues(b.backend).Inc()
	openUntilNs := b.openUntil.Load()
	if openUntilNs == 0 {
		// Closed — fast path.
		return true
	}
	now := time.Now().UnixNano()
	if now < openUntilNs {
		// Still open; reject.
		return false
	}
	// Cooldown elapsed → try to grab the half-open trial slot. CAS ensures
	// exactly one concurrent caller wins; the rest see halfOpen==true and
	// bounce.
	if b.halfOpen.CompareAndSwap(false, true) {
		breakerState.WithLabelValues(b.backend).Set(float64(StateHalfOpen))
		return true
	}
	return false
}

// isCallerDeadline reports whether err is a caller-deadline cancellation
// — context.Canceled or context.DeadlineExceeded. These errors are filtered
// OUT of breaker failure accounting because the upstream caller (the api
// process) gave up; the downstream backend wasn't necessarily slow or
// broken. Counting them would let a busy provisioner with healthy backends
// trip its own breakers solely because callers kept timing out.
//
// gRPC also surfaces these as codes.Canceled and codes.DeadlineExceeded
// wrapped in a status.Error — those still satisfy errors.Is because
// status.Error's Unwrap chain preserves context.{Canceled,DeadlineExceeded}
// when the cancellation came from the request context. The audit-mandated
// behaviour ("filter context.Canceled + context.DeadlineExceeded out of the
// failure record") is exactly that errors.Is check.
func isCallerDeadline(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Record feeds the outcome of an attempt back into the breaker.
//
//   - err == nil: success. Resets the consecutive-failure counter. If the
//     breaker was in half-open, transitions to closed.
//   - err is a caller-deadline (context.Canceled / context.DeadlineExceeded):
//     IGNORED for the purposes of state. The breaker neither counts nor
//     resets; the in-flight half-open slot is released so the next genuine
//     attempt can take it. See isCallerDeadline.
//   - err != nil and not a caller deadline: failure. Increments consecutive;
//     if threshold is crossed, transitions to open and arms the cooldown.
//     A half-open trial failure re-opens immediately.
//
// Record MUST NOT be called when Allow() returned false.
func (b *Breaker) Record(err error) {
	if err == nil {
		// Success — reset consecutive counter. If we were in half-open,
		// close the breaker fully.
		b.consecutive.Store(0)
		if b.halfOpen.CompareAndSwap(true, false) {
			b.openUntil.Store(0)
			breakerState.WithLabelValues(b.backend).Set(float64(StateClosed))
			slog.Info("circuit.closed",
				"backend", b.backend,
				"reason", "half_open_trial_succeeded",
			)
		}
		return
	}

	// Caller-deadline filter (audit P1-1 fix class): a caller that gave up
	// is not a downstream fault. Release the half-open slot if we're in one
	// — the trial didn't actually exercise the downstream — and bail
	// without touching the consecutive counter or failure metrics.
	if isCallerDeadline(err) {
		if b.halfOpen.Load() {
			// We held the trial slot but the request never reached
			// completion against the downstream. Release the slot without
			// counting a failure; the cooldown stays armed so the NEXT
			// genuine caller gets the trial.
			b.halfOpen.Store(false)
			// Re-arm the open state visually — Allow() will compute it from
			// openUntil. Nothing to do here other than ensure the gauge
			// reflects "open" rather than "half_open".
			breakerState.WithLabelValues(b.backend).Set(float64(StateOpen))
		}
		return
	}

	breakerFailures.WithLabelValues(b.backend).Inc()

	// If we're in half-open, the trial counts as the failure that re-opens
	// us — restart cooldown and bail.
	if b.halfOpen.Load() {
		b.halfOpen.Store(false)
		b.consecutive.Store(0)
		b.openUntil.Store(time.Now().Add(b.cooldown).UnixNano())
		breakerOpens.WithLabelValues(b.backend).Inc()
		breakerState.WithLabelValues(b.backend).Set(float64(StateOpen))
		slog.Warn("circuit.reopened",
			"backend", b.backend,
			"reason", "half_open_trial_failed",
			"cooldown_seconds", int(b.cooldown.Seconds()),
		)
		if b.onOpen != nil {
			b.onOpen()
		}
		return
	}

	n := b.consecutive.Add(1)
	if n < b.threshold {
		return
	}
	now := time.Now()
	until := now.Add(b.cooldown).UnixNano()
	if b.openUntil.CompareAndSwap(0, until) {
		breakerOpens.WithLabelValues(b.backend).Inc()
		breakerState.WithLabelValues(b.backend).Set(float64(StateOpen))
		slog.Warn("circuit.opened",
			"backend", b.backend,
			"reason", "consecutive_failure_threshold_crossed",
			"threshold", b.threshold,
			"cooldown_seconds", int(b.cooldown.Seconds()),
		)
		if b.onOpen != nil {
			b.onOpen()
		}
	}
}

// State returns the breaker's current state (closed / open / half_open).
// Computed live from the atomic fields — no lock needed.
func (b *Breaker) State() State {
	if b.halfOpen.Load() {
		return StateHalfOpen
	}
	openUntilNs := b.openUntil.Load()
	if openUntilNs == 0 {
		return StateClosed
	}
	if time.Now().UnixNano() < openUntilNs {
		return StateOpen
	}
	// Cooldown elapsed but no Allow() has grabbed the trial slot yet —
	// from the dashboard's POV we're still open until something probes us.
	return StateOpen
}

// Backend returns the breaker's metric-label name. Used by tests and the
// server wrapper's structured slog calls.
func (b *Breaker) Backend() string { return b.backend }

// Do is a small helper that wraps a function call with Allow() / Record().
// Returns ErrOpen if the breaker is open; otherwise runs `fn`, records the
// outcome, and returns fn's error verbatim. Most provisioner call sites
// use this rather than calling Allow / Record directly so the breaker
// integration stays a single line.
//
// Example:
//
//	err := breakers.PostgresAdmin.Do(func() error {
//	    return b.postgresBackend.Provision(ctx, token, tier, conn)
//	})
func (b *Breaker) Do(fn func() error) error {
	if !b.Allow() {
		return ErrOpen
	}
	err := fn()
	b.Record(err)
	return err
}
