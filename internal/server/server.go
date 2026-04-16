package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/backend/storage"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/pool"
)

// Server implements ProvisionerServiceServer.
type Server struct {
	provisionerv1.UnimplementedProvisionerServiceServer
	postgresBackend          postgres.Backend
	redisBackend             redis.Backend
	mongoBackend             mongo.Backend
	queueBackend             queue.Backend         // shared NATS backend (all tiers)
	storageBackend           *storage.MinIOBackend // nil if MinIO not configured; used for StorageBytes queries
	dedicatedPostgresBackend postgres.Backend      // nil if not configured; used for Pro/Team tier
	dedicatedRedisBackend    redis.Backend         // nil if not configured; used for Pro/Team tier
	dedicatedMongoBackend    mongo.Backend         // nil if not configured; used for Pro/Team tier
	dedicatedQueueBackend    queue.Backend         // nil if not configured; used for Pro/Team tier
	pool                     *pool.Manager         // nil if pool is disabled
}

// New creates a Server with backends and optional pool manager.
// Dedicated backend priority (highest to lowest):
//  1. K8S_DEDICATED_BACKEND=true → k8s pod per token (Team tier)
//  2. DEDICATED_POSTGRES_DSN / NEON_API_KEY → Neon or local admin DSN
//  3. nil → shared backend used for all tiers
func New(cfg *config.Config, poolMgr *pool.Manager) *Server {
	var dedPG postgres.Backend
	var dedRedis redis.Backend
	var dedMongo mongo.Backend
	var dedQueue queue.Backend

	if cfg.K8sDedicatedBackend {
		// k8s backend: each Pro/Team-tier token gets its own namespace + pod.
		// K8S_EXTERNAL_HOST must be set; provisioner fails to start if missing.
		if cfg.K8sExternalHost == "" {
			slog.Error("provisioner: K8S_DEDICATED_BACKEND=true but K8S_EXTERNAL_HOST is not set — " +
				"set K8S_EXTERNAL_HOST to the node IP (local dev) or NLB DNS (EKS production)")
		} else {
			var err error
			dedPG, err = postgres.NewK8sDedicatedBackend(cfg.K8sKubeconfig, cfg.K8sStorageClass, cfg.K8sPostgresImage, cfg.K8sExternalHost, cfg.K8sPostgresStorageGi)
			if err != nil {
				slog.Error("provisioner: k8s postgres backend init failed", "error", err)
			}
			dedRedis, err = redis.NewK8sDedicatedBackend(cfg.K8sKubeconfig, cfg.K8sStorageClass, cfg.K8sRedisImage, cfg.K8sExternalHost, cfg.K8sRedisStorageGi)
			if err != nil {
				slog.Error("provisioner: k8s redis backend init failed", "error", err)
			}
			dedMongo, err = mongo.NewK8sDedicatedBackend(cfg.K8sKubeconfig, cfg.K8sStorageClass, cfg.K8sMongoImage, cfg.K8sExternalHost, cfg.K8sMongoStorageGi)
			if err != nil {
				slog.Error("provisioner: k8s mongo backend init failed", "error", err)
			}
			dedQueue, err = queue.NewK8sDedicatedBackend(cfg.K8sKubeconfig, cfg.K8sNatsImage, cfg.K8sExternalHost)
			if err != nil {
				slog.Error("provisioner: k8s nats backend init failed", "error", err)
			}
			slog.Info("provisioner: k8s dedicated backend enabled",
				"external_host", cfg.K8sExternalHost,
				"storage_class", cfg.K8sStorageClass,
			)
		}
	} else {
		// Legacy dedicated backends (Neon / Upstash / shared admin DSN).
		if cfg.DedicatedPostgresDSN != "" || cfg.NeonAPIKey != "" {
			dedPG = postgres.NewDedicatedBackend(cfg.DedicatedPostgresDSN, cfg.NeonAPIKey)
		}
		if cfg.DedicatedRedisURL != "" || cfg.UpstashAPIKey != "" {
			dedRedis = redis.NewDedicatedBackend(cfg.DedicatedRedisURL, cfg.UpstashAPIKey)
		}
	}

	var minioBackend *storage.MinIOBackend
	if cfg.MinioEndpoint != "" {
		var err error
		minioBackend, err = storage.New(cfg.MinioEndpoint, cfg.MinioRootUser, cfg.MinioRootPassword, cfg.MinioBucketName)
		if err != nil {
			slog.Warn("provisioner: MinIO backend init failed — storage StorageBytes will return 0", "error", err)
		}
	}

	return NewWithBackends(
		cfg,
		postgres.NewBackend(cfg.PostgresProvisionBackend, cfg.PostgresCustomersURL, cfg.PostgresClusterURLs, cfg.NeonAPIKey, cfg.NeonRegionID),
		redis.NewBackend(cfg.RedisProvisionBackend, cfg.RedisProvisionHost),
		mongo.NewBackend(cfg.MongoAdminURI, cfg.MongoHost),
		queue.NewBackend(cfg.NATSHost),
		minioBackend,
		dedPG,
		dedRedis,
		dedMongo,
		dedQueue,
		poolMgr,
	)
}

// NewWithBackends creates a Server with explicitly provided backends.
// Used in tests to inject mock backends.
// Dedicated backends may be nil; when nil the shared backend is used for all tiers.
func NewWithBackends(
	cfg *config.Config,
	pgB postgres.Backend,
	rB redis.Backend,
	mB mongo.Backend,
	qB queue.Backend,
	storageB *storage.MinIOBackend,
	dedicatedPG postgres.Backend,
	dedicatedRedis redis.Backend,
	dedicatedMongo mongo.Backend,
	dedicatedQueue queue.Backend,
	poolMgr *pool.Manager,
) *Server {
	return &Server{
		postgresBackend:          pgB,
		redisBackend:             rB,
		mongoBackend:             mB,
		queueBackend:             qB,
		storageBackend:           storageB,
		dedicatedPostgresBackend: dedicatedPG,
		dedicatedRedisBackend:    dedicatedRedis,
		dedicatedMongoBackend:    dedicatedMongo,
		dedicatedQueueBackend:    dedicatedQueue,
		pool:                     poolMgr,
	}
}

// PostgresBackend returns the shared Postgres backend. Used by main to start
// optional lifecycle goroutines (e.g. ClusterRouter polling) via type assertion.
func (s *Server) PostgresBackend() postgres.Backend {
	return s.postgresBackend
}

// ProvisionResource provisions a resource of the requested type.
// It first tries to claim a pre-provisioned item from the pool.
// If the pool is empty or disabled, it falls back to live provisioning.
func (s *Server) ProvisionResource(ctx context.Context, req *provisionerv1.ProvisionRequest) (*provisionerv1.ProvisionResponse, error) {
	if req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	ctx, span := otel.Tracer("instant.dev/provisioner").Start(ctx, "ProvisionResource",
		trace.WithAttributes(
			attribute.String("resource_type", req.ResourceType.String()),
			attribute.String("tier", req.Tier),
			attribute.String("resource.token", req.Token),
		),
	)
	defer span.End()

	slog.Info("server.ProvisionResource",
		"token", req.Token,
		"resource_type", req.ResourceType,
		"tier", req.Tier,
		"request_id", req.RequestId,
	)

	switch req.ResourceType {
	case commonv1.ResourceType_RESOURCE_TYPE_POSTGRES:
		return s.provisionPostgres(ctx, req)
	case commonv1.ResourceType_RESOURCE_TYPE_REDIS:
		return s.provisionRedis(ctx, req)
	case commonv1.ResourceType_RESOURCE_TYPE_MONGODB:
		return s.provisionMongo(ctx, req)
	case commonv1.ResourceType_RESOURCE_TYPE_QUEUE:
		return s.provisionQueue(ctx, req)
	case commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED:
		return nil, status.Error(codes.InvalidArgument, "resource_type is unspecified")
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown resource_type: %v", req.ResourceType)
	}
}

// isDedicatedTier returns true for tiers that receive dedicated k8s infrastructure
// (one pod/namespace per token) rather than shared cluster resources.
func isDedicatedTier(tier string) bool {
	return tier == "pro" || tier == "team" || tier == "growth"
}

func (s *Server) provisionPostgres(ctx context.Context, req *provisionerv1.ProvisionRequest) (*provisionerv1.ProvisionResponse, error) {
	// Pro and team tiers with a configured dedicated backend skip the shared pool entirely.
	if isDedicatedTier(req.Tier) && s.dedicatedPostgresBackend != nil {
		slog.Info("server.provisionPostgres: using dedicated backend", "token", req.Token, "tier", req.Tier)
		creds, err := s.dedicatedPostgresBackend.Provision(ctx, req.Token, req.Tier)
		if err != nil {
			return nil, mapError("ProvisionResource.postgres.dedicated", err)
		}
		return &provisionerv1.ProvisionResponse{
			ConnectionUrl:      creds.URL,
			ProviderResourceId: creds.ProviderResourceID,
			DatabaseName:       creds.DatabaseName,
			Username:           creds.Username,
		}, nil
	}

	// Try pool first (shared tiers only).
	if s.pool != nil {
		item, err := s.pool.Claim(ctx, "postgres")
		if err != nil {
			slog.Warn("server.provisionPostgres: pool claim error (falling back to live)", "error", err)
		} else if item != nil {
			slog.Info("server.provisionPostgres: pool hit", "pool_id", item.ID)
			return &provisionerv1.ProvisionResponse{
				ConnectionUrl:      item.ConnectionURL,
				ProviderResourceId: item.ProviderResourceID,
				DatabaseName:       item.DatabaseName,
				Username:           item.Username,
			}, nil
		}
	}

	// Pool miss — live provision.
	slog.Info("server.provisionPostgres: pool miss, provisioning live", "token", req.Token)
	creds, err := s.postgresBackend.Provision(ctx, req.Token, req.Tier)
	if err != nil {
		return nil, mapError("ProvisionResource.postgres", err)
	}
	return &provisionerv1.ProvisionResponse{
		ConnectionUrl:      creds.URL,
		ProviderResourceId: creds.ProviderResourceID,
		DatabaseName:       creds.DatabaseName,
		Username:           creds.Username,
	}, nil
}

func (s *Server) provisionRedis(ctx context.Context, req *provisionerv1.ProvisionRequest) (*provisionerv1.ProvisionResponse, error) {
	// Pro and team tiers with a configured dedicated backend skip the shared pool entirely.
	if isDedicatedTier(req.Tier) && s.dedicatedRedisBackend != nil {
		slog.Info("server.provisionRedis: using dedicated backend", "token", req.Token, "tier", req.Tier)
		creds, err := s.dedicatedRedisBackend.Provision(ctx, req.Token, req.Tier)
		if err != nil {
			return nil, mapError("ProvisionResource.redis.dedicated", err)
		}
		return &provisionerv1.ProvisionResponse{
			ConnectionUrl:      creds.URL,
			KeyPrefix:          creds.KeyPrefix,
			ProviderResourceId: creds.ProviderResourceID,
		}, nil
	}

	if s.pool != nil {
		item, err := s.pool.Claim(ctx, "redis")
		if err != nil {
			slog.Warn("server.provisionRedis: pool claim error (falling back to live)", "error", err)
		} else if item != nil {
			slog.Info("server.provisionRedis: pool hit", "pool_id", item.ID)
			return &provisionerv1.ProvisionResponse{
				ConnectionUrl: item.ConnectionURL,
				KeyPrefix:     item.KeyPrefix,
			}, nil
		}
	}

	slog.Info("server.provisionRedis: pool miss, provisioning live", "token", req.Token)
	creds, err := s.redisBackend.Provision(ctx, req.Token, req.Tier)
	if err != nil {
		return nil, mapError("ProvisionResource.redis", err)
	}
	return &provisionerv1.ProvisionResponse{
		ConnectionUrl: creds.URL,
		KeyPrefix:     creds.KeyPrefix,
	}, nil
}

func (s *Server) provisionMongo(ctx context.Context, req *provisionerv1.ProvisionRequest) (*provisionerv1.ProvisionResponse, error) {
	// Pro and team tiers with a configured dedicated backend skip the shared pool entirely.
	if isDedicatedTier(req.Tier) && s.dedicatedMongoBackend != nil {
		slog.Info("server.provisionMongo: using dedicated backend", "token", req.Token, "tier", req.Tier)
		creds, err := s.dedicatedMongoBackend.Provision(ctx, req.Token, req.Tier)
		if err != nil {
			return nil, mapError("ProvisionResource.mongo.dedicated", err)
		}
		return &provisionerv1.ProvisionResponse{
			ConnectionUrl:      creds.URL,
			ProviderResourceId: creds.ProviderResourceID,
			DatabaseName:       creds.DatabaseName,
		}, nil
	}

	if s.pool != nil {
		item, err := s.pool.Claim(ctx, "mongodb")
		if err != nil {
			slog.Warn("server.provisionMongo: pool claim error (falling back to live)", "error", err)
		} else if item != nil {
			slog.Info("server.provisionMongo: pool hit", "pool_id", item.ID)
			return &provisionerv1.ProvisionResponse{
				ConnectionUrl: item.ConnectionURL,
				DatabaseName:  item.DatabaseName,
			}, nil
		}
	}

	slog.Info("server.provisionMongo: pool miss, provisioning live", "token", req.Token)
	creds, err := s.mongoBackend.Provision(ctx, req.Token, req.Tier)
	if err != nil {
		return nil, mapError("ProvisionResource.mongo", err)
	}
	return &provisionerv1.ProvisionResponse{
		ConnectionUrl:      creds.URL,
		ProviderResourceId: creds.ProviderResourceID,
		DatabaseName:       creds.DatabaseName,
	}, nil
}

func (s *Server) provisionQueue(ctx context.Context, req *provisionerv1.ProvisionRequest) (*provisionerv1.ProvisionResponse, error) {
	// Pro and team tiers with a dedicated k8s backend get their own NATS pod.
	if isDedicatedTier(req.Tier) && s.dedicatedQueueBackend != nil {
		slog.Info("server.provisionQueue: using dedicated backend", "token", req.Token, "tier", req.Tier)
		creds, err := s.dedicatedQueueBackend.Provision(ctx, req.Token, req.Tier)
		if err != nil {
			return nil, mapError("ProvisionResource.queue.dedicated", err)
		}
		return &provisionerv1.ProvisionResponse{
			ConnectionUrl:      creds.URL,
			ProviderResourceId: creds.ProviderResourceID,
			KeyPrefix:          creds.SubjectPrefix,
		}, nil
	}

	slog.Info("server.provisionQueue: using shared backend", "token", req.Token)
	creds, err := s.queueBackend.Provision(ctx, req.Token, req.Tier)
	if err != nil {
		return nil, mapError("ProvisionResource.queue", err)
	}
	return &provisionerv1.ProvisionResponse{
		ConnectionUrl: creds.URL,
		KeyPrefix:     creds.SubjectPrefix,
	}, nil
}

// DeprovisionResource deprovisions a resource of the requested type.
func (s *Server) DeprovisionResource(ctx context.Context, req *provisionerv1.DeprovisionRequest) (*provisionerv1.DeprovisionResponse, error) {
	if req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	ctx, span := otel.Tracer("instant.dev/provisioner").Start(ctx, "DeprovisionResource",
		trace.WithAttributes(
			attribute.String("resource_type", req.ResourceType.String()),
			attribute.String("resource.token", req.Token),
		),
	)
	defer span.End()

	slog.Info("server.DeprovisionResource",
		"token", req.Token,
		"resource_type", req.ResourceType,
		"provider_resource_id", req.ProviderResourceId,
		"request_id", req.RequestId,
	)

	switch req.ResourceType {
	case commonv1.ResourceType_RESOURCE_TYPE_POSTGRES:
		// Route to dedicated backend when a providerResourceID is present (Neon project ID)
		// or when the dedicated backend is configured for local-dedicated resources.
		// providerResourceID is non-empty for Neon-provisioned projects; empty for local.
		if s.dedicatedPostgresBackend != nil && req.ProviderResourceId != "" {
			slog.Info("server.DeprovisionResource: postgres using dedicated backend",
				"token", req.Token, "provider_resource_id", req.ProviderResourceId)
			if err := s.dedicatedPostgresBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
				return nil, mapError("DeprovisionResource.postgres.dedicated", err)
			}
			return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil
		}
		if err := s.postgresBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
			return nil, mapError("DeprovisionResource.postgres", err)
		}
		return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_REDIS:
		// Dedicated Redis deprovision: providerResourceID holds Upstash DB ID when set.
		if s.dedicatedRedisBackend != nil && req.ProviderResourceId != "" {
			slog.Info("server.DeprovisionResource: redis using dedicated backend",
				"token", req.Token, "provider_resource_id", req.ProviderResourceId)
			if err := s.dedicatedRedisBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
				return nil, mapError("DeprovisionResource.redis.dedicated", err)
			}
			return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil
		}
		if err := s.redisBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
			return nil, mapError("DeprovisionResource.redis", err)
		}
		return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_MONGODB:
		if s.dedicatedMongoBackend != nil && req.ProviderResourceId != "" {
			slog.Info("server.DeprovisionResource: mongo using dedicated backend",
				"token", req.Token, "provider_resource_id", req.ProviderResourceId)
			if err := s.dedicatedMongoBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
				return nil, mapError("DeprovisionResource.mongo.dedicated", err)
			}
			return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil
		}
		if err := s.mongoBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
			return nil, mapError("DeprovisionResource.mongo", err)
		}
		return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_QUEUE:
		if s.dedicatedQueueBackend != nil && req.ProviderResourceId != "" {
			slog.Info("server.DeprovisionResource: queue using dedicated backend",
				"token", req.Token, "provider_resource_id", req.ProviderResourceId)
			if err := s.dedicatedQueueBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
				return nil, mapError("DeprovisionResource.queue.dedicated", err)
			}
			return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil
		}
		if err := s.queueBackend.Deprovision(ctx, req.Token, req.ProviderResourceId); err != nil {
			return nil, mapError("DeprovisionResource.queue", err)
		}
		return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED:
		return nil, status.Error(codes.InvalidArgument, "resource_type is unspecified")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown resource_type: %v", req.ResourceType)
	}
}

// GetStorageBytes returns the storage used by a resource.
func (s *Server) GetStorageBytes(ctx context.Context, req *provisionerv1.StorageRequest) (*provisionerv1.StorageResponse, error) {
	if req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	slog.Info("server.GetStorageBytes",
		"token", req.Token,
		"resource_type", req.ResourceType,
		"provider_resource_id", req.ProviderResourceId,
	)

	switch req.ResourceType {
	case commonv1.ResourceType_RESOURCE_TYPE_POSTGRES:
		// Route to dedicated backend when a providerResourceID is present (Neon project ID).
		if s.dedicatedPostgresBackend != nil && req.ProviderResourceId != "" {
			bytes, err := s.dedicatedPostgresBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
			if err != nil {
				return nil, mapError("GetStorageBytes.postgres.dedicated", err)
			}
			return &provisionerv1.StorageResponse{
				StorageBytes: bytes,
				MeasuredAt:   time.Now().Unix(),
			}, nil
		}
		bytes, err := s.postgresBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
		if err != nil {
			return nil, mapError("GetStorageBytes.postgres", err)
		}
		return &provisionerv1.StorageResponse{
			StorageBytes: bytes,
			MeasuredAt:   time.Now().Unix(),
		}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_REDIS:
		// Route to dedicated backend when a providerResourceID is present (Upstash DB ID).
		if s.dedicatedRedisBackend != nil && req.ProviderResourceId != "" {
			bytes, err := s.dedicatedRedisBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
			if err != nil {
				return nil, mapError("GetStorageBytes.redis.dedicated", err)
			}
			return &provisionerv1.StorageResponse{
				StorageBytes: bytes,
				MeasuredAt:   time.Now().Unix(),
			}, nil
		}
		bytes, err := s.redisBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
		if err != nil {
			return nil, mapError("GetStorageBytes.redis", err)
		}
		return &provisionerv1.StorageResponse{
			StorageBytes: bytes,
			MeasuredAt:   time.Now().Unix(),
		}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_MONGODB:
		bytes, err := s.mongoBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
		if err != nil {
			return nil, mapError("GetStorageBytes.mongo", err)
		}
		return &provisionerv1.StorageResponse{
			StorageBytes: bytes,
			MeasuredAt:   time.Now().Unix(),
		}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_STORAGE:
		if s.storageBackend == nil {
			slog.Warn("server.GetStorageBytes.storage: MinIO backend not configured — returning 0")
			return &provisionerv1.StorageResponse{StorageBytes: 0, MeasuredAt: time.Now().Unix()}, nil
		}
		bytes, err := s.storageBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
		if err != nil {
			return nil, mapError("GetStorageBytes.storage", err)
		}
		return &provisionerv1.StorageResponse{
			StorageBytes: bytes,
			MeasuredAt:   time.Now().Unix(),
		}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED:
		return nil, status.Error(codes.InvalidArgument, "resource_type is unspecified")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown resource_type: %v", req.ResourceType)
	}
}

// mapError converts backend errors to appropriate gRPC status codes.
func mapError(op string, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	slog.Error(fmt.Sprintf("server.%s", op), "error", msg)

	if isConnectError(msg) {
		return status.Errorf(codes.Unavailable, "%s: connect failure: %v", op, err)
	}
	if isAlreadyExistsError(msg) {
		return status.Errorf(codes.AlreadyExists, "%s: already exists: %v", op, err)
	}
	if isInvalidArgError(msg) {
		return status.Errorf(codes.InvalidArgument, "%s: invalid argument: %v", op, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", op, err)
}

func isConnectError(msg string) bool {
	for _, kw := range []string{"connect", "connection refused", "no such host", "dial", "timeout", "unavailable"} {
		if strings.Contains(strings.ToLower(msg), kw) {
			return true
		}
	}
	return false
}

func isAlreadyExistsError(msg string) bool {
	for _, kw := range []string{"already exists", "duplicate"} {
		if strings.Contains(strings.ToLower(msg), kw) {
			return true
		}
	}
	return false
}

func isInvalidArgError(msg string) bool {
	return strings.Contains(strings.ToLower(msg), "empty") ||
		strings.Contains(strings.ToLower(msg), "invalid")
}
