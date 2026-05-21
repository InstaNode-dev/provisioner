// Command provisioner is the gRPC service that creates/destroys real
// customer databases, caches, queues, and storage prefixes on behalf of
// the api service.
//
// Observability — relocated 2026-05-12 from the api repo's reference scaffold
// (track B2 of the observability rollout). What this file wires up:
//
//   1. slog default handler decorated with instant.dev/common/logctx so every
//      log line carries service / commit_id / trace_id / tid / team_id.
//   2. New Relic Go agent (fail-open: an unset NEW_RELIC_LICENSE_KEY logs a
//      warning and returns nil; a nil app is safe to pass to nrgrpc).
//   3. nrgrpc.UnaryServerInterceptor chained with a trace-id stamper so
//      W3C-propagated trace IDs reach downstream slog calls in handlers.
//   4. HTTP sidecar on :8092 exposing /healthz with build metadata JSON.
//      Same shape as api and worker /healthz so a single jq filter works.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/newrelic/go-agent/v3/integrations/nrgrpc"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/common/crypto"
	"instant.dev/common/logctx"
	"instant.dev/provisioner/internal/circuit"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/handlers"
	"instant.dev/provisioner/internal/interceptor"
	"instant.dev/provisioner/internal/pool"
	"instant.dev/provisioner/internal/server"
	"instant.dev/provisioner/internal/telemetry"
)

// breakerAdapter wraps a *circuit.Breaker so it satisfies the
// handlers.CircuitInspector interface (Name() / IsOpen()). The breaker's
// own API is Backend() string + State() State for historical reasons; this
// adapter renames+maps without changing the breaker surface every other
// consumer relies on. A tripped breaker (StateOpen) shows up on /readyz as
// the corresponding `backend_<name>` check in degraded state — exactly the
// signal an operator needs to see "one downstream is sick, the pod is
// staying in rotation while it half-opens".
type breakerAdapter struct{ b *circuit.Breaker }

func (a breakerAdapter) Name() string { return a.b.Backend() }
func (a breakerAdapter) IsOpen() bool { return a.b.State() == circuit.StateOpen }

// collectBreakerInspectors returns the per-backend breakers as the
// CircuitInspector slice the readyz handler wants. nil-safe — if `bs` is
// nil (test path that constructs the server without breakers) returns nil
// and the handler registers zero backend_* checks.
func collectBreakerInspectors(bs *circuit.Breakers) []handlers.CircuitInspector {
	if bs == nil {
		return nil
	}
	return []handlers.CircuitInspector{
		breakerAdapter{b: bs.PostgresAdmin},
		breakerAdapter{b: bs.PostgresK8s},
		breakerAdapter{b: bs.RedisAdmin},
		breakerAdapter{b: bs.MongoAdmin},
		breakerAdapter{b: bs.K8sAPI},
	}
}

// healthzAddr is the listen address for the HTTP sidecar. Port 8092 was
// chosen by the rollout plan because it doesn't collide with the gRPC port
// (50051), the api Fiber port (8080), Prometheus scrapers in our cluster
// (9090, 9091, 9100), or any of the data-namespace services. See
// TestHealthzPortNoCollisionWithGRPC for the assertion.
const healthzAddr = ":8092"

// initNewRelic boots the New Relic Go agent. It is fail-open: an empty
// license key (the common case in dev) or any initialization error logs a
// warning and returns nil. Callers must handle a nil *newrelic.Application
// — the nrgrpc interceptor does so safely.
func initNewRelic() *newrelic.Application {
	licenseKey := os.Getenv("NEW_RELIC_LICENSE_KEY")
	if licenseKey == "" {
		slog.Warn("newrelic.disabled — NEW_RELIC_LICENSE_KEY not set")
		return nil
	}

	appName := os.Getenv("NEW_RELIC_APP_NAME")
	if appName == "" {
		appName = "instant-provisioner"
	}

	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName(appName),
		newrelic.ConfigLicense(licenseKey),
		newrelic.ConfigDistributedTracerEnabled(true),
		newrelic.ConfigAppLogForwardingEnabled(true),
	)
	if err != nil {
		// Fail-open: log and continue. A provisioning outage because the NR
		// agent couldn't dial home would be a wildly disproportionate failure
		// mode for an observability dependency.
		slog.Warn("newrelic.init_failed", "error", err)
		return nil
	}
	return app
}

// composeTraceIDInjector wraps an inner interceptor (typically
// nrgrpc.UnaryServerInterceptor) so that after the inner one has opened the
// NR transaction on ctx, we stamp the trace ID onto ctx via logctx for
// downstream slog calls. Extracted to package-private function so tests can
// invoke it without standing up a real gRPC server.
func composeTraceIDInjector(inner grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		wrapped := func(nrCtx context.Context, nrReq any) (any, error) {
			return handler(stampTraceIDFromNR(nrCtx), nrReq)
		}
		return inner(ctx, req, info, wrapped)
	}
}

// stampTraceIDFromNR looks up the NR transaction on ctx (placed there by
// nrgrpc.UnaryServerInterceptor) and, if present, copies its trace ID onto
// ctx via logctx.WithTraceID. Safe to call when no NR transaction is on
// ctx — returns ctx unchanged.
//
// Split out of composeTraceIDInjector to be unit-testable: a test can
// pre-populate ctx with newrelic.NewContext(ctx, txn) and assert the
// trace_id ends up on the returned ctx. Tests against the *bare* function
// (without spinning up a gRPC server) keep CI fast.
func stampTraceIDFromNR(ctx context.Context) context.Context {
	txn := newrelic.FromContext(ctx)
	if txn == nil {
		return ctx
	}
	md := txn.GetTraceMetadata()
	if md.TraceID == "" {
		return ctx
	}
	return logctx.WithTraceID(ctx, md.TraceID)
}

// poolBox is a goroutine-safe holder for the lazily-initialized
// pgxpool. The HTTP sidecar binds before the pool exists (so /healthz
// stays available during pool boot), but /readyz needs to probe the
// pool once it's up. The box bridges the two: at boot, /readyz reports
// platform_db=failed; after the pool is set, /readyz starts pinging it.
type poolBox struct {
	mu   sync.Mutex
	pool *pgxpool.Pool
}

func (b *poolBox) Get() *pgxpool.Pool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pool
}

func (b *poolBox) Set(p *pgxpool.Pool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pool = p
}

// poolProbe adapts the box to the handlers.Config.Pool API by lazily
// fetching the pool at probe time.
type poolProbe struct{ box *poolBox }

func (p poolProbe) Ping(ctx context.Context) error {
	pool := p.box.Get()
	if pool == nil {
		return errPoolNotReady
	}
	return pool.Ping(ctx)
}

var errPoolNotReady = errors.New("pool_not_ready")

// startHealthzSidecar starts the HTTP server on healthzAddr in a goroutine.
// Returns the *http.Server so the caller can shut it down cleanly. The
// listener errors are logged but never crash the process — losing /healthz
// should not take down provisioning.
//
// The readiness gate is wired into the handler so /healthz reports ok:false
// (HTTP 503) until the gRPC listener is up and again once shutdown begins,
// instead of an unconditional ok:true the instant the HTTP sidecar binds.
//
// readyz is the new deep readiness probe — see internal/handlers/readyz.go.
// poolBox starts holding nil and is populated by main once the pgxpool is up;
// /readyz reports platform_db=failed with LastError="pgxpool_not_configured"
// until that happens.
func startHealthzSidecar(ready *server.Readiness, box *poolBox, poolEnabled bool) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/healthz", server.HealthzHandler(ready))

	// /readyz — wraps the box so the pool can be set after the sidecar
	// is bound. The handler is constructed here (once) with a Pool=nil;
	// the deep check function inside readyzPoolPing reaches into the
	// box at probe time so the check transitions from failed → ok the
	// moment main.Box.Set() runs.
	//
	// poolEnabled gates whether platform_db is even a registered check.
	// When the operator intentionally runs without PROVISIONER_DATABASE_URL
	// (hot pool disabled — gRPC provisioning works fine without it because
	// the provisioner reads its own state from k8s + customer DB, not from
	// the provisioner's platform DB), reporting platform_db=failed-critical
	// → 503 forever is a false alarm. It pulls both pods out of the Service
	// endpoint list (k8s readinessProbe == 503), silences every NR alert
	// keyed off provisioner metrics (because /metrics goes with /readyz),
	// and turns the api's deep-readyz provisioner_grpc check red. When
	// the pool is intentionally disabled, omit platform_db entirely; when
	// PROVISIONER_DATABASE_URL is set but Set() hasn't fired yet, the
	// existing failed-critical signal still applies (BugBash B14-P0-F2).
	cfg := handlers.Config{
		PoolPinger: poolProbe{box: box},
		// Wire the per-backend circuit breakers so /readyz surfaces a
		// tripped breaker as `backend_<name>` degraded. The breakers run
		// from circuit.Default (package-level) — same instance the gRPC
		// server's callBackend wraps every backend dispatch with. A
		// tripped breaker means the upstream is failing fast: surface
		// that on /readyz as degraded (not failed — the breaker exists
		// precisely so the provisioner can stay up while one backend is
		// sick; pulling the pod from rotation would defeat the breaker).
		// BugBash 2026-05-20 — `Circuits` had been silently empty since
		// the deep /readyz shipped, so the breaker state was invisible
		// to anything except logs.
		Circuits: collectBreakerInspectors(circuit.Default),
	}
	if !poolEnabled {
		cfg.PoolPinger = nil // tells NewReadyzHandler to skip the check
	}
	readyzH := handlers.NewReadyzHandler(cfg)
	mux.HandleFunc("/readyz", readyzH.Get)

	// /metrics — expose the process's default Prometheus registry so
	// the cluster ServiceMonitor scrapes circuit breaker state, backend
	// latency histograms, readyz_check_status gauge, and the standard
	// Go runtime collectors. Without this handler, every NR alert keyed
	// off provisioner_* metrics is silently dead (BugBash B14-P0-F1).
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              healthzAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("healthz.listening", "addr", healthzAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("healthz.serve_failed", "error", err)
		}
	}()

	return srv
}

func main() {
	// First action: install the obs-enriching slog handler as the default
	// so every log line from boot onward carries service / commit_id and
	// the empty-string-stable trace_id / tid / team_id fields. The bare slog
	// default that the provisioner previously used emitted unstructured-ish
	// records — this is the inconsistency the plan flagged.
	slog.SetDefault(slog.New(logctx.NewHandler(
		"provisioner",
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelInfo,
		}),
	)))

	// Boot NR before any other slog calls that might want to be traced.
	nrApp := initNewRelic()
	defer func() {
		if nrApp != nil {
			nrApp.Shutdown(10 * time.Second)
		}
	}()

	// Start the HTTP /healthz sidecar early so k8s readiness probes (track 5
	// switches them from gRPC tcpSocket to HTTP) can see commit_id even
	// while the gRPC server is still booting backends. The readiness gate
	// starts "not ready" and flips true only once the gRPC listener binds
	// below, so /healthz does not lie ok:true during backend boot.
	//
	// poolHolder is the lazy slot the /readyz handler reads at probe
	// time. Bound to the box BEFORE the sidecar starts so the wire
	// shape is stable from the first probe; the box's pool field stays
	// nil until the pgxpool succeeds below.
	readiness := &server.Readiness{}
	poolHolder := &poolBox{}
	// poolEnabled mirrors the hot-pool startup condition below. We
	// compute it BEFORE starting the sidecar so /readyz semantics are
	// stable from the first probe: if the pool is disabled by config,
	// platform_db is not a registered check (no /readyz 503 forever);
	// if it's enabled, the check is registered and starts in
	// "pgxpool_not_configured" → flips to ok once poolHolder.Set fires.
	cfg := config.Load()
	poolEnabled := cfg.ProvisionerDatabaseURL != "" && cfg.AESKey != ""
	healthzSrv := startHealthzSidecar(readiness, poolHolder, poolEnabled)

	shutdownTracer := telemetry.InitTracer("instant-provisioner", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	defer func() {
		if err := shutdownTracer(context.Background()); err != nil {
			slog.Error("telemetry.shutdown_failed", "error", err)
		}
	}()

	// P1-M: fail closed on a missing/empty PROVISIONER_SECRET. An
	// unauthenticated provisioner is a remote create/destroy-database surface;
	// it must refuse to start rather than silently disable auth. The k8s
	// deployment supplies PROVISIONER_SECRET from the instant-infra-secrets
	// Secret; for local `make run` the operator must export it explicitly.
	if err := interceptor.ValidateSecret(cfg.ProvisionerSecret); err != nil {
		slog.Error("provisioner.auth_misconfigured",
			"error", err,
			"remediation", "set PROVISIONER_SECRET (k8s: instant-infra-secrets; local: export PROVISIONER_SECRET=$(openssl rand -hex 32))")
		os.Exit(1)
	}

	// --- optional hot-pool ---
	var poolMgr *pool.Manager
	if cfg.ProvisionerDatabaseURL != "" && cfg.AESKey != "" {
		aesKey, err := crypto.ParseAESKey(cfg.AESKey)
		if err != nil {
			slog.Error("provisioner.aes_key_parse_failed", "error", err)
			os.Exit(1)
		}

		// Wave-3 chaos verify (2026-05-21): use bounded pgxpool config
		// instead of pgxpool.New's defaults (default MaxConns =
		// max(4, runtime.NumCPU()) which under-bounds on big-CPU
		// nodes). newBoundedPgxPoolConfig also reads
		// PROVISIONER_PG_MAX_CONNS / PROVISIONER_PG_MIN_CONNS /
		// PROVISIONER_PG_CONN_MAX_LIFETIME / PROVISIONER_PG_CONN_MAX_IDLE_TIME
		// so the operator can raise the ceiling per-environment.
		pgxCfg, err := newBoundedPgxPoolConfig(cfg.ProvisionerDatabaseURL)
		if err != nil {
			slog.Error("provisioner.pool_db_parse_failed", "error", err)
			os.Exit(1)
		}
		slog.Info("provisioner.pool_db_config_resolved",
			"max_conns", pgxCfg.MaxConns,
			"min_conns", pgxCfg.MinConns,
			"max_conn_lifetime", pgxCfg.MaxConnLifetime.String(),
			"max_conn_idle_time", pgxCfg.MaxConnIdleTime.String(),
		)
		dbPool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
		if err != nil {
			slog.Error("provisioner.pool_db_connect_failed", "error", err)
			os.Exit(1)
		}

		// Pool-saturation observability. Goroutine ticks every 5s
		// and pushes pgxpool.Stat onto instant_pg_pool_* Prometheus
		// gauges. Lives for the process lifetime; the cancel function
		// is wired into the existing shutdown path via context.
		poolStatsCtx, poolStatsCancel := context.WithCancel(context.Background())
		defer poolStatsCancel()
		go startPgxPoolStatsExporter(poolStatsCtx, dbPool, "provisioner_db")

		// Verify connectivity with retries — k3s/Flannel sometimes needs a moment
		// to establish ClusterIP routing in a freshly-started container.
		const pingAttempts = 5
		var pingErr error
		for attempt := range pingAttempts {
			pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			pingErr = dbPool.Ping(pingCtx)
			cancel()
			if pingErr == nil {
				break
			}
			backoff := time.Duration(attempt+1) * 500 * time.Millisecond
			slog.Warn("provisioner.pool_db_ping_retry",
				"attempt", attempt+1, "of", pingAttempts,
				"backoff_ms", backoff.Milliseconds(),
				"error", pingErr,
			)
			time.Sleep(backoff)
		}
		if pingErr != nil {
			slog.Warn("provisioner.pool_db_ping_failed — pool disabled", "error", pingErr)
		} else {
			// Pool is up — surface it on /readyz via the lazy box so the
			// platform_db check transitions from failed → ok. Done BEFORE
			// pool.Manager.Start so a slow refill doesn't keep /readyz red
			// (the DB is reachable; the refill is async).
			poolHolder.Set(dbPool)

			// Build pool manager — it shares the same backends as the server.
			// The server's New() also initialises its own backend instances;
			// pool uses separate instances pointing to the same infrastructure.
			// This is intentional: pool refill runs concurrently with request handling.
			poolCfg := pool.Config{
				PostgresSize: cfg.PoolPostgresSize,
				RedisSize:    cfg.PoolRedisSize,
				MongoSize:    cfg.PoolMongoSize,
				QueueSize:    cfg.PoolQueueSize,
			}

			poolMgr = pool.NewWithConfig(dbPool, aesKey, poolCfg, cfg)
			if err := poolMgr.Start(context.Background()); err != nil {
				slog.Error("provisioner.pool_start_failed", "error", err)
				os.Exit(1)
			}
			slog.Info("provisioner.pool_enabled",
				"postgres_target", cfg.PoolPostgresSize,
				"redis_target", cfg.PoolRedisSize,
				"mongo_target", cfg.PoolMongoSize,
				"queue_target", cfg.PoolQueueSize,
			)
		}
	} else {
		slog.Info("provisioner.pool_disabled — PROVISIONER_DATABASE_URL or AES_KEY not set")
	}

	srv := server.New(cfg, poolMgr)

	// Start background cluster-capacity polling on the Postgres backend when
	// it exposes the optional Starter interface (LocalBackend with ClusterRouter).
	type starter interface{ Start(ctx context.Context) }
	if pgStarter, ok := srv.PostgresBackend().(starter); ok {
		pgStarter.Start(context.Background())
		slog.Info("provisioner.cluster_router_started")
	}

	// Chain the unary interceptors:
	//   1. auth (PROVISIONER_SECRET) — runs first, rejects unauthenticated calls
	//   2. nrgrpc — opens the NR transaction + propagates W3C TraceContext
	//   3. trace-id stamper — copies the NR trace ID onto ctx via logctx so
	//      handler slog calls log with trace_id
	//
	// grpc.ChainUnaryInterceptor preserves order: first interceptor wraps
	// the second, which wraps the third, which wraps the handler.
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			interceptor.UnaryAuthInterceptor(cfg.ProvisionerSecret),
			composeTraceIDInjector(nrgrpc.UnaryServerInterceptor(nrApp)),
		),
		// Allow client keepalive pings every 15s (client sends every 20s — within policy).
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             15 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
		}),
	)

	provisionerv1.RegisterProvisionerServiceServer(grpcServer, srv)

	// Register the standard grpc.health.v1 service. The api's /readyz
	// uses this to probe the provisioner — see api/internal/provisioner/
	// client.go HealthCheck(). We mark the empty-string service ("")
	// SERVING immediately because the gRPC listener is about to bind
	// below; if a future refactor moves the bind earlier or later,
	// this status flip should move with it. (The HTTP sidecar's
	// readiness gate is the source of truth for HTTP probes; this is
	// the gRPC mirror.)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthSrv)

	lis, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		slog.Error("provisioner.listen_failed", "port", cfg.Port, "error", err)
		os.Exit(1)
	}

	slog.Info("provisioner.starting", "port", cfg.Port, "healthz_addr", healthzAddr)

	// The TCP listener is bound and the gRPC server is about to serve — the
	// process is ready to accept provisioning RPCs. Flip the readiness gate
	// so /healthz reports ok:true.
	readiness.SetReady(true)

	// Serve gRPC in a goroutine so we can also handle SIGTERM cleanly and
	// shut the /healthz sidecar down too. The previous main.go blocked on
	// grpcServer.Serve directly; that worked, but left the HTTP server
	// orphaned at shutdown. With the chained shutdown below the pod's
	// terminationGracePeriodSeconds is more predictable.
	serveErr := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			serveErr <- err
		}
		close(serveErr)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("provisioner.shutdown_signal", "signal", sig.String())
	case err := <-serveErr:
		if err != nil {
			slog.Error("provisioner.serve_failed", "error", err)
		}
	}

	// Flip readiness off first so any /healthz probe arriving during the
	// drain window reports ok:false (503) and k8s pulls the pod out of the
	// Service endpoints before in-flight gRPC calls are torn down.
	readiness.SetReady(false)
	// Mirror to the gRPC health server so any in-flight grpc.health.v1
	// probe from the api gets NOT_SERVING and the api's /readyz turns
	// the provisioner_grpc check red before our actual RPC teardown.
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	// Graceful shutdown of both surfaces. GracefulStop on grpc.Server drains
	// in-flight calls; Shutdown on the HTTP server gives /healthz a 5s window.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := healthzSrv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("healthz.shutdown_error", "error", err)
	}
	grpcServer.GracefulStop()

	if poolMgr != nil {
		poolMgr.Shutdown()
	}
}
