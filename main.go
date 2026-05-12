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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/newrelic/go-agent/v3/integrations/nrgrpc"
	"github.com/newrelic/go-agent/v3/newrelic"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/common/crypto"
	"instant.dev/common/logctx"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/interceptor"
	"instant.dev/provisioner/internal/pool"
	"instant.dev/provisioner/internal/server"
	"instant.dev/provisioner/internal/telemetry"
)

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

// startHealthzSidecar starts the HTTP server on healthzAddr in a goroutine.
// Returns the *http.Server so the caller can shut it down cleanly. The
// listener errors are logged but never crash the process — losing /healthz
// should not take down provisioning.
func startHealthzSidecar() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/healthz", server.HealthzHandler())

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
	// while the gRPC server is still booting backends.
	healthzSrv := startHealthzSidecar()

	shutdownTracer := telemetry.InitTracer("instant-provisioner", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	defer func() {
		if err := shutdownTracer(context.Background()); err != nil {
			slog.Error("telemetry.shutdown_failed", "error", err)
		}
	}()

	cfg := config.Load()

	// --- optional hot-pool ---
	var poolMgr *pool.Manager
	if cfg.ProvisionerDatabaseURL != "" && cfg.AESKey != "" {
		aesKey, err := crypto.ParseAESKey(cfg.AESKey)
		if err != nil {
			slog.Error("provisioner.aes_key_parse_failed", "error", err)
			os.Exit(1)
		}

		dbPool, err := pgxpool.New(context.Background(), cfg.ProvisionerDatabaseURL)
		if err != nil {
			slog.Error("provisioner.pool_db_connect_failed", "error", err)
			os.Exit(1)
		}

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

	lis, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		slog.Error("provisioner.listen_failed", "port", cfg.Port, "error", err)
		os.Exit(1)
	}

	slog.Info("provisioner.starting", "port", cfg.Port, "healthz_addr", healthzAddr)

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
