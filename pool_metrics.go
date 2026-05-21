package main

// pool_metrics.go — bounded pgxpool config + saturation metrics for the
// provisioner's hot-pool database connection. Wave-3 chaos verify
// (2026-05-21): a 50-concurrent api /db/new burst exhausted the shared
// DigitalOcean Managed Postgres user-connection ceiling. The
// provisioner's own pgxpool wasn't the proximate cause (it talks to
// PROVISIONER_DATABASE_URL on a different DO host — REDACTED —
// not the platform DB), but the same pattern can recur on that host
// once the hot-pool churns under load; this file extends the same
// observability + bounded-pool discipline applied in api and worker.

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Pool-size defaults.
//
// Provisioner's database is a workhorse Postgres at REDACTED used
// for hot-pool tracking + cluster routing. Unlike the api/worker
// platform_db, this host is a single DO Droplet (not Managed PG with
// its slot reservations) — so the per-process pool ceiling matters
// less for upstream-saturation reasons and more for "don't open more
// conns than the workload actually needs" reasons. Default 10/3 is
// generous for the workload (hot-pool refill + the occasional gRPC
// handler INSERT).
const (
	defaultProvisionerPGMaxConns    = 10
	defaultProvisionerPGMinConns    = 2
	defaultProvisionerPGConnMaxLife = 4 * time.Minute
	defaultProvisionerPGConnMaxIdle = 90 * time.Second
)

// Pool-saturation gauges. Provisioner has no `internal/metrics`
// package; declare these in main and the existing /metrics endpoint
// (mux.Handle("/metrics", promhttp.Handler())) picks them up via the
// default registry.
var (
	pgPoolMaxGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_max",
		Help: "pgxpool MaxConns ceiling on the provisioner pool. Constant for the process lifetime.",
	}, []string{"pool"})

	pgPoolTotalGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_open",
		Help: "pgxpool total connections (acquired + idle + constructing). Sampled every 5s.",
	}, []string{"pool"})

	pgPoolAcquiredGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_in_use",
		Help: "pgxpool connections currently in use. Sampled every 5s. Wave-3 chaos verify 2026-05-21.",
	}, []string{"pool"})

	pgPoolIdleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_idle",
		Help: "pgxpool connections currently idle. Sampled every 5s.",
	}, []string{"pool"})

	pgPoolAcquireWaitCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_wait_count",
		Help: "Cumulative count of acquire-waits since process start (pgxpool.Stat.EmptyAcquireCount). Steepening slope == pool saturated.",
	}, []string{"pool"})

	pgPoolAcquireDurationSecs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_wait_duration_seconds",
		Help: "Cumulative time spent in acquire-waits since process start (pgxpool.Stat.AcquireDuration).",
	}, []string{"pool"})

	pgPoolCanceledAcquireCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_canceled_acquire_count",
		Help: "Cumulative acquire-cancels since process start (pgxpool.Stat.CanceledAcquireCount). A non-zero rate means handlers are timing out before connections become available.",
	}, []string{"pool"})
)

// envInt32 reads a positive int32 from an env var, falling back to def.
// Bad values fall back too — provisioner must not refuse to start on a typo.
func envInt32(name string, def int32) int32 {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n <= 0 {
		return def
	}
	return int32(n)
}

// envDuration reads a Go time.Duration from an env var, falling back to def.
func envDuration(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// newBoundedPgxPoolConfig parses a pgxpool config from a DSN and
// applies the bounded defaults (overridable via env). Returns a
// configured *pgxpool.Config ready to pass to pgxpool.NewWithConfig.
//
// Env vars:
//
//	PROVISIONER_PG_MAX_CONNS         (default 10)
//	PROVISIONER_PG_MIN_CONNS         (default 2)
//	PROVISIONER_PG_CONN_MAX_LIFETIME (default 4m)
//	PROVISIONER_PG_CONN_MAX_IDLE_TIME (default 90s)
func newBoundedPgxPoolConfig(dsn string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = envInt32("PROVISIONER_PG_MAX_CONNS", defaultProvisionerPGMaxConns)
	cfg.MinConns = envInt32("PROVISIONER_PG_MIN_CONNS", defaultProvisionerPGMinConns)
	cfg.MaxConnLifetime = envDuration("PROVISIONER_PG_CONN_MAX_LIFETIME", defaultProvisionerPGConnMaxLife)
	cfg.MaxConnIdleTime = envDuration("PROVISIONER_PG_CONN_MAX_IDLE_TIME", defaultProvisionerPGConnMaxIdle)

	return cfg, nil
}

// startPgxPoolStatsExporter samples pgxpool.Stat every 5s and pushes
// the relevant numbers onto the instant_pg_pool_* gauges. Blocks
// until ctx is cancelled. Mirror of api/internal/db/pool_metrics.go
// for pgxpool semantics.
func startPgxPoolStatsExporter(ctx context.Context, pool *pgxpool.Pool, label string) {
	if pool == nil {
		slog.Warn("provisioner.pool_metrics.skip — nil pool", "label", label)
		return
	}

	const interval = 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("provisioner.pool_metrics.exporter_started",
		"label", label,
		"interval", interval.String(),
	)

	publishPgxPoolStats(pool, label)

	for {
		select {
		case <-ctx.Done():
			slog.Info("provisioner.pool_metrics.exporter_stopped", "label", label)
			return
		case <-ticker.C:
			publishPgxPoolStats(pool, label)
		}
	}
}

// publishPgxPoolStats reads pool.Stat() and pushes onto the gauges.
// Exported as a free function so tests can drive it directly without
// spinning a ticker.
func publishPgxPoolStats(pool *pgxpool.Pool, label string) {
	s := pool.Stat()
	pgPoolMaxGauge.WithLabelValues(label).Set(float64(s.MaxConns()))
	pgPoolTotalGauge.WithLabelValues(label).Set(float64(s.TotalConns()))
	pgPoolAcquiredGauge.WithLabelValues(label).Set(float64(s.AcquiredConns()))
	pgPoolIdleGauge.WithLabelValues(label).Set(float64(s.IdleConns()))
	pgPoolAcquireWaitCount.WithLabelValues(label).Set(float64(s.EmptyAcquireCount()))
	pgPoolAcquireDurationSecs.WithLabelValues(label).Set(s.AcquireDuration().Seconds())
	pgPoolCanceledAcquireCount.WithLabelValues(label).Set(float64(s.CanceledAcquireCount()))
}
