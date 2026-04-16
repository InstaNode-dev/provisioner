package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/common/crypto"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/interceptor"
	"instant.dev/provisioner/internal/pool"
	"instant.dev/provisioner/internal/server"
	"instant.dev/provisioner/internal/telemetry"
)

func main() {
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

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(interceptor.UnaryAuthInterceptor(cfg.ProvisionerSecret)),
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

	slog.Info("provisioner.starting", "port", cfg.Port)
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("provisioner.serve_failed", "error", err)
		os.Exit(1)
	}

	if poolMgr != nil {
		poolMgr.Shutdown()
	}
}
