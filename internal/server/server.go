package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"instant.dev/common/plans"
	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/backend/storage"
	"instant.dev/provisioner/internal/circuit"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/ctxkeys"
	"instant.dev/provisioner/internal/pool"
	"instant.dev/provisioner/internal/poolident"
)

// teamIDMetaKey is the gRPC metadata key used to pass the owning team UUID
// from the API to the provisioner. The provisioner uses this value to label
// dedicated (k8s-backed) namespaces with instant.dev/owner-team so the
// deploy-side NetworkPolicy can scope DB-port egress per-team.
// Lowercase: gRPC metadata keys are canonically lowercase.
const teamIDMetaKey = "x-instant-team-id"

// teamIDFromContext extracts the team ID transmitted via gRPC metadata.
// Returns empty string when the key is absent (anonymous provisions).
func teamIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(teamIDMetaKey)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// PoolClaimer is the minimal Claim surface server.go uses from
// *pool.Manager. Extracting it lets tests inject a fake claimer that
// returns synthesised pool hits / errors without standing up a real
// *pgxpool.Pool — *pool.Manager itself remains the production
// implementation. Keep this interface byte-for-byte aligned with
// (*pool.Manager).Claim so the concrete type continues to satisfy it.
type PoolClaimer interface {
	Claim(ctx context.Context, resourceType string) (*pool.Item, error)
}

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
	pool                     PoolClaimer           // nil if pool is disabled; production type is *pool.Manager

	// breakers — per-backend in-process circuit breakers (audit P0-3).
	// Independent instance per backend so a Redis outage cannot trip the
	// Postgres breaker. See internal/circuit/breakers.go for the full set
	// (postgres_k8s, postgres_admin, redis_admin, mongo_admin, k8s_api).
	//
	// Tests construct a fresh Breakers via circuit.NewBreakers() and pass
	// it through NewWithBackends so per-test breaker state stays local;
	// production wires this to circuit.Default in New().
	breakers *circuit.Breakers
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
		mongo.NewBackend(cfg.MongoProvisionBackend, cfg.MongoAdminURI, cfg.MongoHost),
		queue.NewBackend(cfg.QueueProvisionBackend, cfg.NATSHost),
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
	// A typed nil *pool.Manager assigned to a PoolClaimer interface field
	// would make `s.pool != nil` evaluate true (the interface holds a
	// non-nil type descriptor) and the next Claim call would dereference a
	// nil receiver. Normalise the nil pointer to a nil interface here so
	// production callers that pass nil get the documented "pool disabled"
	// behaviour.
	var poolClaimer PoolClaimer
	if poolMgr != nil {
		poolClaimer = poolMgr
	}
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
		pool:                     poolClaimer,
		// circuit.Default is the process-wide breaker set. Tests inject
		// their own via SetBreakers() (or NewServerForTest) so per-test
		// trips don't bleed into other tests.
		breakers: circuit.Default,
	}
}

// SetBreakers replaces the breaker set used by every backend dispatch path.
// Test-only entry point — production code wires circuit.Default in
// NewWithBackends and never calls this. Returns the server for chaining.
func (s *Server) SetBreakers(b *circuit.Breakers) *Server {
	s.breakers = b
	return s
}

// SetPool injects an alternate pool claimer. Test-only entry point — production
// code wires *pool.Manager via NewWithBackends and never calls this. Useful
// for exercising the pool-hit / pool-error / pool-miss branches in
// provisionPostgres/Redis/Mongo/Queue without standing up a real *pgxpool.Pool.
func (s *Server) SetPool(p PoolClaimer) *Server {
	s.pool = p
	return s
}

// breakerForPostgres returns the right breaker for a Postgres call: the
// `postgres_k8s` breaker when the dedicated k8s backend will be used (Pro/
// Team/Growth tier with a configured dedicated backend), and the
// `postgres_admin` breaker for shared-cluster calls through LocalBackend.
//
// Routing here MUST match the same isDedicatedTier + non-nil-backend gate
// the provision/deprovision/storage handlers use — otherwise a call wrapped
// against `postgres_k8s` could end up dispatched to `local.go` (or vice
// versa) and the breaker would protect the wrong downstream.
//
// For deprovision/storage paths that dispatch on PRID prefix
// ("instant-customer-" → dedicated k8s, else shared) the caller passes
// `useDedicated=true` directly.
func (s *Server) breakerForPostgres(useDedicated bool) *circuit.Breaker {
	if useDedicated {
		return s.breakers.PostgresK8s
	}
	return s.breakers.PostgresAdmin
}

// breakerForRedis returns the right breaker for a Redis call. The shared
// LocalBackend manipulates the redis-provision ACL (the "Redis admin"
// surface in the brief); the dedicated k8s backend issues raw kube-
// apiserver calls (the "k8s_api" surface).
func (s *Server) breakerForRedis(useDedicated bool) *circuit.Breaker {
	if useDedicated {
		return s.breakers.K8sAPI
	}
	return s.breakers.RedisAdmin
}

// breakerForMongo — shared Mongo backend issues admin CREATE USER on the
// shared cluster (`mongo_admin`); dedicated k8s backend issues kube-
// apiserver calls (`k8s_api`).
func (s *Server) breakerForMongo(useDedicated bool) *circuit.Breaker {
	if useDedicated {
		return s.breakers.K8sAPI
	}
	return s.breakers.MongoAdmin
}

// callBackend wraps a backend invocation behind the given breaker. Returns
// circuit.ErrOpen verbatim when the breaker is open so the caller can map
// it to a gRPC Unavailable via mapError. The breaker's caller-deadline
// filter (audit P1-1) is built into circuit.Breaker.Record itself; a fn
// that returns context.Canceled / context.DeadlineExceeded will NOT count
// toward the failure threshold.
func callBackend[T any](b *circuit.Breaker, fn func() (T, error)) (T, error) {
	var zero T
	if !b.Allow() {
		return zero, circuit.ErrOpen
	}
	v, err := fn()
	b.Record(err)
	return v, err
}

// callBackendVoid is the no-return-value flavour of callBackend, used by
// Deprovision paths whose backend methods return only error.
func callBackendVoid(b *circuit.Breaker, fn func() error) error {
	if !b.Allow() {
		return circuit.ErrOpen
	}
	err := fn()
	b.Record(err)
	return err
}

// PostgresBackend returns the shared Postgres backend. Used by main to start
// optional lifecycle goroutines (e.g. ClusterRouter polling) via type assertion.
func (s *Server) PostgresBackend() postgres.Backend {
	return s.postgresBackend
}

// Breakers returns the per-backend circuit-breaker set. Used by main.go to
// wire each breaker as a `backend_<name>` check on the /readyz handler so a
// tripped breaker surfaces as a degraded readiness component (not failed —
// the breaker's job is to keep the provisioner serving while one backend
// is sick; pulling the pod from rotation would defeat that). Returns nil
// if the server was constructed without breakers (test path); main.go
// callers should range over a nil slice safely.
func (s *Server) Breakers() *circuit.Breakers { return s.breakers }

// ProvisionResource provisions a resource of the requested type.
// It first tries to claim a pre-provisioned item from the pool.
// If the pool is empty or disabled, it falls back to live provisioning.
func (s *Server) ProvisionResource(ctx context.Context, req *provisionerv1.ProvisionRequest) (*provisionerv1.ProvisionResponse, error) {
	if req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	// Extract the owning team ID from gRPC metadata and inject it into the
	// context so the k8s backends can label the namespace with
	// instant.dev/owner-team. This closes the cross-tenant network-isolation
	// gap confirmed by pentest on 2026-05-16.
	if teamID := teamIDFromContext(ctx); teamID != "" {
		ctx = context.WithValue(ctx, ctxkeys.TeamIDKey, teamID)
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
	// Resolve the connection limit for this tier from the shared plans registry.
	// -1 = unlimited (team/growth); > 0 = enforced cap at the Postgres role level.
	// The provisioner owns the plans registry so the cap stays consistent with
	// what RegradeResource applies on plan upgrades.
	connLimit := regradeConnLimits.ConnectionsLimit(req.Tier, "postgres")

	// Pro and team tiers with a configured dedicated backend skip the shared pool entirely.
	if isDedicatedTier(req.Tier) && s.dedicatedPostgresBackend != nil {
		slog.Info("server.provisionPostgres: using dedicated backend", "token", req.Token, "tier", req.Tier)
		// Breaker: postgres_k8s — dedicated k8s Postgres pod ops.
		creds, err := callBackend(s.breakerForPostgres(true), func() (*postgres.Credentials, error) {
			return s.dedicatedPostgresBackend.Provision(ctx, req.Token, req.Tier, connLimit)
		})
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

			// P1-A: pool items are pre-provisioned with connLimit=-1 (unlimited).
			// The pool-claim path used to return the item verbatim, so every
			// anonymous (and any pool-hit paid-tier) Postgres role had NO
			// connection cap — the advertised `connections` limit was a false
			// promise. Apply the claiming tier's CONNECTION LIMIT synchronously
			// here via the same ALTER ROLE path RegradeResource uses, so we do
			// not rely on the async entitlement reconciler catching up later.
			//
			// postgres.Regrade reconstructs the db_/usr_ names from the pool
			// naming token. item.PoolToken is the authoritative value the pool
			// manager stamped at pre-provision time (also used 2 lines below in
			// poolident.Encode); use it directly rather than re-deriving the
			// token by stripping a prefix off item.Username, which drifts the
			// moment the username scheme changes.
			if poolToken := item.PoolToken; poolToken != "" {
				res, rerr := s.postgresBackend.Regrade(ctx, poolToken, item.ProviderResourceID, connLimit)
				if rerr != nil {
					// Fail closed: a pool item we cannot cap must not be handed
					// out as if it were capped. Fall through to live provisioning,
					// which applies the cap at CREATE USER time.
					slog.Warn("server.provisionPostgres: pool item connection-limit regrade failed (falling back to live)",
						"pool_id", item.ID, "tier", req.Tier, "conn_limit", connLimit, "error", rerr)
				} else {
					slog.Info("server.provisionPostgres: pool item connection limit applied",
						"pool_id", item.ID, "tier", req.Tier,
						"conn_limit", connLimit, "applied", res.Applied,
						"applied_conn_limit", res.AppliedConnLimit)
					// P0-2: the backing db_/usr_ are named from the pool token,
					// not req.Token. Encode the pool token into provider_resource_id
					// (alongside the existing "local:<N>" cluster segment) so
					// Deprovision / StorageBytes / Regrade re-derive the real
					// db_pool-<uuid> / usr_pool-<uuid> names and do not leak it.
					return &provisionerv1.ProvisionResponse{
						ConnectionUrl:      item.ConnectionURL,
						ProviderResourceId: poolident.Encode(item.ProviderResourceID, item.PoolToken),
						DatabaseName:       item.DatabaseName,
						Username:           item.Username,
					}, nil
				}
			} else {
				// Pool item carries no naming token — we cannot derive the
				// db_/usr_ names to target the ALTER ROLE. Fall back to live
				// provision rather than hand out an uncapped role.
				slog.Warn("server.provisionPostgres: pool item missing PoolToken — cannot apply connection limit, falling back to live",
					"pool_id", item.ID, "username", item.Username)
			}
		}
	}

	// Pool miss — live provision.
	slog.Info("server.provisionPostgres: pool miss, provisioning live", "token", req.Token, "conn_limit", connLimit)
	// Breaker: postgres_admin — shared postgres-customers CREATE DATABASE /
	// CREATE USER (local.go) when LocalBackend is the active type; the
	// dedicated k8s backend (when wired here as a fallback in unusual
	// configs) gets the same `postgres_admin` breaker because it would be
	// hitting the SHARED cluster — the dedicated case above is already
	// dispatched to `postgres_k8s`.
	creds, err := callBackend(s.breakerForPostgres(false), func() (*postgres.Credentials, error) {
		return s.postgresBackend.Provision(ctx, req.Token, req.Tier, connLimit)
	})
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
		// Breaker: k8s_api — dedicated Redis pod ops issue kube-apiserver calls.
		creds, err := callBackend(s.breakerForRedis(true), func() (*redis.Credentials, error) {
			return s.dedicatedRedisBackend.Provision(ctx, req.Token, req.Tier)
		})
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
			// P0-2: the ACL user (usr_pool-<uuid>) and keyspace (pool-<uuid>:*)
			// are named from the pool token, not req.Token. Encode the pool token
			// into provider_resource_id so Deprovision / StorageBytes re-derive
			// the real names instead of no-op'ing on usr_<real-token>.
			return &provisionerv1.ProvisionResponse{
				ConnectionUrl:      item.ConnectionURL,
				KeyPrefix:          item.KeyPrefix,
				ProviderResourceId: poolident.Encode(item.ProviderResourceID, item.PoolToken),
			}, nil
		}
	}

	slog.Info("server.provisionRedis: pool miss, provisioning live", "token", req.Token)
	// Breaker: redis_admin — shared Redis ACL provider (local.go) operations.
	creds, err := callBackend(s.breakerForRedis(false), func() (*redis.Credentials, error) {
		return s.redisBackend.Provision(ctx, req.Token, req.Tier)
	})
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
		// Breaker: k8s_api — dedicated Mongo pod ops go through kube-apiserver.
		creds, err := callBackend(s.breakerForMongo(true), func() (*mongo.Credentials, error) {
			return s.dedicatedMongoBackend.Provision(ctx, req.Token, req.Tier)
		})
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
			// P0-2: db_pool-<uuid> / usr_pool-<uuid> are named from the pool
			// token, not req.Token. Encode the pool token into
			// provider_resource_id so Deprovision / StorageBytes re-derive the
			// real names instead of no-op'ing on db_<real-token>.
			return &provisionerv1.ProvisionResponse{
				ConnectionUrl:      item.ConnectionURL,
				DatabaseName:       item.DatabaseName,
				ProviderResourceId: poolident.Encode(item.ProviderResourceID, item.PoolToken),
			}, nil
		}
	}

	slog.Info("server.provisionMongo: pool miss, provisioning live", "token", req.Token)
	// Breaker: mongo_admin — shared MongoDB CREATE USER / role grants.
	creds, err := callBackend(s.breakerForMongo(false), func() (*mongo.Credentials, error) {
		return s.mongoBackend.Provision(ctx, req.Token, req.Tier)
	})
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

	// Try the warm pool first — anonymous tier should feel instant. Pool items
	// are pre-provisioned dedicated NATS pods carrying their own connection URL.
	if s.pool != nil {
		item, err := s.pool.Claim(ctx, "queue")
		if err != nil {
			slog.Warn("server.provisionQueue: pool claim error (falling back to live)", "error", err)
		} else if item != nil {
			slog.Info("server.provisionQueue: pool hit", "pool_id", item.ID)
			return &provisionerv1.ProvisionResponse{
				ConnectionUrl:      item.ConnectionURL,
				ProviderResourceId: item.ProviderResourceID,
			}, nil
		}
	}

	slog.Info("server.provisionQueue: pool miss, provisioning live", "token", req.Token)
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
		// Route to dedicated backend ONLY for legacy Neon-style provider IDs.
		// k8s namespace IDs (prefix "instant-customer-") go through the regular
		// postgresBackend which holds the route-registry connection — otherwise
		// the redis route key for this resource never gets unregistered on
		// teardown and the proxy keeps a stale entry forever.
		if s.dedicatedPostgresBackend != nil && req.ProviderResourceId != "" &&
			!strings.HasPrefix(req.ProviderResourceId, "instant-customer-") {
			slog.Info("server.DeprovisionResource: postgres using dedicated backend",
				"token", req.Token, "provider_resource_id", req.ProviderResourceId)
			// Breaker: postgres_k8s — dedicated Postgres teardown.
			if err := callBackendVoid(s.breakerForPostgres(true), func() error {
				return s.dedicatedPostgresBackend.Deprovision(ctx, req.Token, req.ProviderResourceId)
			}); err != nil {
				return nil, mapError("DeprovisionResource.postgres.dedicated", err)
			}
			return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil
		}
		// Breaker: postgres_admin — shared cluster DROP DATABASE / DROP USER.
		if err := callBackendVoid(s.breakerForPostgres(false), func() error {
			return s.postgresBackend.Deprovision(ctx, req.Token, req.ProviderResourceId)
		}); err != nil {
			return nil, mapError("DeprovisionResource.postgres", err)
		}
		return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_REDIS:
		// Skip dedicated backend for k8s-style provider IDs so the regular
		// redisBackend's route-registry connection unregisters the route key.
		if s.dedicatedRedisBackend != nil && req.ProviderResourceId != "" &&
			!strings.HasPrefix(req.ProviderResourceId, "instant-customer-") {
			slog.Info("server.DeprovisionResource: redis using dedicated backend",
				"token", req.Token, "provider_resource_id", req.ProviderResourceId)
			// Breaker: k8s_api — dedicated Redis pod teardown via kube-apiserver.
			if err := callBackendVoid(s.breakerForRedis(true), func() error {
				return s.dedicatedRedisBackend.Deprovision(ctx, req.Token, req.ProviderResourceId)
			}); err != nil {
				return nil, mapError("DeprovisionResource.redis.dedicated", err)
			}
			return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil
		}
		// Breaker: redis_admin — shared Redis ACL DELUSER / namespace cleanup.
		if err := callBackendVoid(s.breakerForRedis(false), func() error {
			return s.redisBackend.Deprovision(ctx, req.Token, req.ProviderResourceId)
		}); err != nil {
			return nil, mapError("DeprovisionResource.redis", err)
		}
		return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_MONGODB:
		if s.dedicatedMongoBackend != nil && req.ProviderResourceId != "" &&
			!strings.HasPrefix(req.ProviderResourceId, "instant-customer-") {
			slog.Info("server.DeprovisionResource: mongo using dedicated backend",
				"token", req.Token, "provider_resource_id", req.ProviderResourceId)
			// Breaker: k8s_api — dedicated Mongo pod teardown via kube-apiserver.
			if err := callBackendVoid(s.breakerForMongo(true), func() error {
				return s.dedicatedMongoBackend.Deprovision(ctx, req.Token, req.ProviderResourceId)
			}); err != nil {
				return nil, mapError("DeprovisionResource.mongo.dedicated", err)
			}
			return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil
		}
		// Breaker: mongo_admin — shared MongoDB DROP USER / DROP DATABASE.
		if err := callBackendVoid(s.breakerForMongo(false), func() error {
			return s.mongoBackend.Deprovision(ctx, req.Token, req.ProviderResourceId)
		}); err != nil {
			return nil, mapError("DeprovisionResource.mongo", err)
		}
		return &provisionerv1.DeprovisionResponse{Deprovisioned: true}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_QUEUE:
		if s.dedicatedQueueBackend != nil && req.ProviderResourceId != "" &&
			!strings.HasPrefix(req.ProviderResourceId, "instant-customer-") {
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
		// Route to dedicated backend when a providerResourceID is present (Neon
		// project ID). P1-W3-12 / P2-W2-06: a pool-claimed SHARED resource also
		// carries a non-empty provider_resource_id ("local:<N>" and/or a
		// "pooltok:" marker) — that is NOT a dedicated resource. The
		// "instant-customer-" guard (matching DeprovisionResource's routing)
		// keeps pool-claimed shared Postgres measured by the shared backend
		// instead of being mis-routed to the dedicated backend, which would
		// measure the wrong instance or fail outright.
		if s.dedicatedPostgresBackend != nil && req.ProviderResourceId != "" &&
			!isSharedBackendProviderID(req.ProviderResourceId) {
			// Breaker: postgres_k8s — dedicated Postgres pg_database_size.
			bytes, err := callBackend(s.breakerForPostgres(true), func() (int64, error) {
				return s.dedicatedPostgresBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
			})
			if err != nil {
				return nil, mapError("GetStorageBytes.postgres.dedicated", err)
			}
			return &provisionerv1.StorageResponse{
				StorageBytes: bytes,
				MeasuredAt:   time.Now().Unix(),
			}, nil
		}
		// Breaker: postgres_admin — shared cluster pg_database_size query.
		bytes, err := callBackend(s.breakerForPostgres(false), func() (int64, error) {
			return s.postgresBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
		})
		if err != nil {
			return nil, mapError("GetStorageBytes.postgres", err)
		}
		return &provisionerv1.StorageResponse{
			StorageBytes: bytes,
			MeasuredAt:   time.Now().Unix(),
		}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_REDIS:
		// Route to dedicated backend when a providerResourceID is present
		// (Upstash DB ID). P1-W3-12 / P2-W2-06: skip the dedicated backend for
		// pool-claimed shared Redis — its PRID carries the "pooltok:" marker —
		// so a pool-claimed resource is measured against the shared instance it
		// actually lives on, not a dedicated instance that does not exist.
		if s.dedicatedRedisBackend != nil && req.ProviderResourceId != "" &&
			!isSharedBackendProviderID(req.ProviderResourceId) {
			// Breaker: k8s_api — dedicated Redis MEMORY USAGE via the pod's kube-exec channel.
			bytes, err := callBackend(s.breakerForRedis(true), func() (int64, error) {
				return s.dedicatedRedisBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
			})
			if err != nil {
				return nil, mapError("GetStorageBytes.redis.dedicated", err)
			}
			return &provisionerv1.StorageResponse{
				StorageBytes: bytes,
				MeasuredAt:   time.Now().Unix(),
			}, nil
		}
		// Breaker: redis_admin — shared Redis MEMORY USAGE / DBSIZE query.
		bytes, err := callBackend(s.breakerForRedis(false), func() (int64, error) {
			return s.redisBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
		})
		if err != nil {
			return nil, mapError("GetStorageBytes.redis", err)
		}
		return &provisionerv1.StorageResponse{
			StorageBytes: bytes,
			MeasuredAt:   time.Now().Unix(),
		}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_MONGODB:
		// Breaker: mongo_admin — shared MongoDB dbStats command.
		bytes, err := callBackend(s.breakerForMongo(false), func() (int64, error) {
			return s.mongoBackend.StorageBytes(ctx, req.Token, req.ProviderResourceId)
		})
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

	case commonv1.ResourceType_RESOURCE_TYPE_QUEUE:
		// P2-W2-05: queues are metered by messages-stored (plans.yaml
		// webhook/queue stored-count limits), not by on-disk bytes — the
		// queue.Backend interface has no StorageBytes method. Returning
		// InvalidArgument here (the pre-fix default-case behaviour) made the
		// worker's storage sweep log a spurious gRPC error for every queue
		// resource on every tick. Return 0 explicitly so the sweep treats
		// queues as a clean zero-byte resource, matching the storage-backend-
		// not-configured fail-open path above.
		return &provisionerv1.StorageResponse{
			StorageBytes: 0,
			MeasuredAt:   time.Now().Unix(),
		}, nil

	case commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED:
		return nil, status.Error(codes.InvalidArgument, "resource_type is unspecified")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown resource_type: %v", req.ResourceType)
	}
}

// regradeConnLimits is the source of truth for per-tier caps used by
// RegradeResource. It is the shared plans registry — the same source the
// agent API uses — so caps re-applied here stay platform-consistent.
// StorageLimitMB(tier, "redis") gives the Redis maxmemory cap in MB.
// ConnectionsLimit(tier, "postgres") gives the Postgres CONNECTION LIMIT.
var regradeConnLimits = plans.Default()

// redisK8sProviderIDPrefix is the namespace prefix assigned to every k8s-backed
// Redis resource. Shared-backend (local/dedicated) resources do not have this
// prefix — their provider_resource_id is either empty or a non-k8s identifier.
// Using a constant here (rather than importing the redis package's private const)
// keeps the dependency clean; both values must stay in sync with k8s.go.
const redisK8sProviderIDPrefix = "instant-customer-"

// localClusterPRIDPrefix is the provider_resource_id form of a shared (local)
// Postgres resource: "local:<clusterIndex>". See postgres/cluster_router.go.
const localClusterPRIDPrefix = "local:"

// isSharedBackendProviderID reports whether a provider_resource_id belongs to a
// SHARED-backend resource rather than a genuine dedicated (Neon/Upstash) one.
//
// A shared resource's PRID is one of:
//   - "" — live-provisioned shared Redis/Mongo
//   - "local:<N>" — shared Postgres cluster index
//   - any value carrying the poolident "pooltok:" marker — a pool-claimed
//     shared resource (e.g. "local:0;pooltok:pool-<uuid>" or "pooltok:pool-<uuid>")
//
// P1-W3-12 / P2-W2-06: GetStorageBytes must not route these to the dedicated
// backend just because the PRID is non-empty — a pool-claimed shared resource
// always has a non-empty PRID. DeprovisionResource already guards on the
// "instant-customer-" k8s prefix; this is the GetStorageBytes-side equivalent,
// expressed positively so a "local:" / "pooltok:" PRID is unambiguously shared.
func isSharedBackendProviderID(providerResourceID string) bool {
	if providerResourceID == "" {
		return true
	}
	if strings.HasPrefix(providerResourceID, localClusterPRIDPrefix) {
		return true
	}
	return strings.Contains(providerResourceID, poolident.Marker)
}

// RegradeResource re-applies the tier's infrastructure cap to an
// already-provisioned resource. It exists because a plan upgrade does not, on
// its own, re-apply the higher cap to the live infrastructure — the resource
// keeps the old (lower) cap until this RPC runs.
//
// Supported resource types:
//   - POSTGRES: re-applies CONNECTION LIMIT on the Postgres role.
//   - REDIS (k8s-backed only): re-applies maxmemory + noeviction policy via
//     CONFIG SET. Shared-backend Redis (no per-tenant cap lever) returns
//     {applied:false} with a descriptive skip_reason.
//
// All other resource types return {applied:false, skip_reason:"unsupported"}.
func (s *Server) RegradeResource(ctx context.Context, req *provisionerv1.RegradeRequest) (*provisionerv1.RegradeResponse, error) {
	if req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	ctx, span := otel.Tracer("instant.dev/provisioner").Start(ctx, "RegradeResource",
		trace.WithAttributes(
			attribute.String("resource_type", req.ResourceType.String()),
			attribute.String("tier", req.Tier),
			attribute.String("resource.token", req.Token),
		),
	)
	defer span.End()

	switch req.ResourceType {
	case commonv1.ResourceType_RESOURCE_TYPE_POSTGRES:
		return s.regradePostgres(ctx, req)
	case commonv1.ResourceType_RESOURCE_TYPE_REDIS:
		return s.regradeRedis(ctx, req)
	default:
		slog.Info("server.RegradeResource",
			"token", req.Token, "tier", req.Tier,
			"resource_type", req.ResourceType.String(),
			"applied", false, "skip_reason", "unsupported resource type for regrade",
			"request_id", req.RequestId)
		return &provisionerv1.RegradeResponse{
			Applied:    false,
			SkipReason: "unsupported resource type for regrade",
		}, nil
	}
}

// regradePostgres re-applies the CONNECTION LIMIT on the customer's Postgres role.
func (s *Server) regradePostgres(ctx context.Context, req *provisionerv1.RegradeRequest) (*provisionerv1.RegradeResponse, error) {
	// Select the backend that actually owns this resource. k8s namespace IDs
	// (prefix "instant-customer-") go through the regular postgresBackend,
	// matching the routing DeprovisionResource uses.
	backend := s.postgresBackend
	if s.dedicatedPostgresBackend != nil && req.ProviderResourceId != "" &&
		!strings.HasPrefix(req.ProviderResourceId, "instant-customer-") {
		backend = s.dedicatedPostgresBackend
	}

	// Connection cap comes from the shared plans registry, keeping it
	// consistent with the cap applied at provision time. -1 = unlimited; passed
	// through verbatim. Both the local (shared cluster) and k8s (dedicated pod)
	// backends implement Regrade — the local backend issues ALTER ROLE, the k8s
	// backend does the same against the pod-local Postgres.
	connLimit := regradeConnLimits.ConnectionsLimit(req.Tier, "postgres")

	// Pick the breaker that matches the backend we just routed to: dedicated
	// k8s backend → `postgres_k8s`; LocalBackend → `postgres_admin`. Mirrors
	// the routing branch above.
	regradeBreaker := s.breakerForPostgres(backend == s.dedicatedPostgresBackend)
	result, err := callBackend(regradeBreaker, func() (postgres.RegradeResult, error) {
		return backend.Regrade(ctx, req.Token, req.ProviderResourceId, connLimit)
	})
	if err != nil {
		return nil, mapError("RegradeResource.postgres", err)
	}

	slog.Info("server.RegradeResource.postgres",
		"token", req.Token, "tier", req.Tier,
		"applied", result.Applied,
		"applied_conn_limit", result.AppliedConnLimit,
		"skip_reason", result.SkipReason,
		"request_id", req.RequestId)

	return &provisionerv1.RegradeResponse{
		Applied:          result.Applied,
		AppliedConnLimit: int32(result.AppliedConnLimit),
		SkipReason:       result.SkipReason,
	}, nil
}

// regradeRedis re-applies maxmemory on a dedicated k8s Redis pod.
//
// Identifier resolution (fix/a4-redis-rekey-on-token):
//   - If ProviderResourceId already has the "instant-customer-" prefix → use it as the
//     k8s namespace name directly (legacy rows that did have prid set, or callers that
//     pass the full namespace).
//   - If ProviderResourceId does NOT have the prefix but is non-empty → treat it as a
//     bare token and construct "instant-customer-<ProviderResourceId>". This is the new
//     path: the worker now passes the token as the identifier because prod rows have
//     provider_resource_id = NULL.
//   - If ProviderResourceId is empty → also construct from req.Token. This mirrors the
//     K8sBackend.Regrade fallback (ns = redisK8sNsPrefix + token when providerResourceID=="").
//
// Safety: only k8s-backed dedicated pods are touched. Shared-backend environments (where
// redisBackend does not implement redis.Regrader) return {applied:false} gracefully so
// the shared redis-provision pod is never accidentally capped.
//
// Team/growth tiers (memory limit = -1 in plans.yaml) set maxmemory=0 (Redis "unlimited")
// so existing pods are never accidentally capped.
func (s *Server) regradeRedis(ctx context.Context, req *provisionerv1.RegradeRequest) (*provisionerv1.RegradeResponse, error) {
	// Resolve the effective provider ID (k8s namespace).
	//   Case 1: prid is already "instant-customer-<something>" → use as-is.
	//   Case 2: prid is a bare token (no prefix, non-empty) → construct namespace.
	//   Case 3: prid is empty → fall back to req.Token to construct namespace.
	// After this block, effectivePRID is either a full "instant-customer-*" namespace
	// or empty (which K8sBackend.Regrade handles by constructing from token).
	// P0-2: strip any pool-token marker first. A pool-claimed shared-Redis
	// resource carries provider_resource_id="pooltok:pool-<uuid>"; without this
	// the namespace construction below would treat the whole marker as a bare
	// token. BasePRID leaves a genuine "instant-customer-*" namespace or
	// "local:<N>" untouched.
	effectivePRID := poolident.BasePRID(req.ProviderResourceId)
	if !strings.HasPrefix(effectivePRID, redisK8sProviderIDPrefix) {
		// Either empty or a bare token. Construct the namespace.
		bareToken := effectivePRID
		if bareToken == "" {
			bareToken = req.Token
		}
		if bareToken != "" {
			effectivePRID = redisK8sProviderIDPrefix + bareToken
		}
		// If both prid and token are empty the guard below (token required) already
		// rejected the request, so effectivePRID="" is reachable only in tests.
	}

	// A non-k8s identifier after resolution means we have no namespace to target —
	// the caller did not provide enough information to locate a dedicated pod.
	// This can happen for truly shared-backend resources (no k8s pod exists).
	// We pass effectivePRID (which now has the prefix) down to the backend; if the
	// namespace does not exist the backend returns a soft skip.
	//
	// The original guard ("skip if no instant-customer- prefix") is preserved in
	// spirit: after the block above, effectivePRID ALWAYS has the prefix when there
	// is any token to work with. An empty effectivePRID here means both prid and
	// token were empty — that cannot happen because RegradeResource already validated
	// req.Token != "" before dispatching here.

	// Resolve the backend that owns this resource. For k8s-style IDs the
	// regular redisBackend holds the route-registry connection; use it here
	// too for consistency with Deprovision routing.
	regrader, ok := s.redisBackend.(redis.Regrader)
	if !ok {
		// redisBackend is not a k8s backend (e.g. local/dedicated in a test env).
		slog.Warn("server.RegradeResource.redis: active redis backend does not support Regrade — skipping",
			"token", req.Token, "tier", req.Tier,
			"effective_prid", effectivePRID,
			"request_id", req.RequestId)
		return &provisionerv1.RegradeResponse{
			Applied:    false,
			SkipReason: "backend does not support redis regrade",
		}, nil
	}

	slog.Info("server.RegradeResource.redis: resolved namespace",
		"token", req.Token, "tier", req.Tier,
		"original_prid", req.ProviderResourceId,
		"effective_prid", effectivePRID,
		"request_id", req.RequestId)

	// Memory cap comes from the shared plans registry.
	// StorageLimitMB("redis") returns plans.yaml redis_memory_mb:
	//   anonymous=5, hobby=50, pro=512, team/growth=-1 (unlimited).
	// -1 (unlimited) → targetMaxmemoryMB=0 → Regrade sets maxmemory=0 (no cap).
	memLimitMB := regradeConnLimits.StorageLimitMB(req.Tier, "redis")
	targetMaxmemoryMB := memLimitMB
	if memLimitMB < 0 {
		// Unlimited tier — explicitly clear any cap on the pod.
		targetMaxmemoryMB = 0
	}

	// Breaker: k8s_api — Regrade on the dedicated Redis pod goes through the
	// kube-apiserver (CONFIG SET via pod-exec or direct AUTH-then-CONFIG-SET).
	result, err := callBackend(s.breakerForRedis(true), func() (redis.RegradeResult, error) {
		return regrader.Regrade(ctx, req.Token, effectivePRID, targetMaxmemoryMB)
	})
	if err != nil {
		return nil, mapError("RegradeResource.redis", err)
	}

	slog.Info("server.RegradeResource.redis",
		"token", req.Token, "tier", req.Tier,
		"applied", result.Applied,
		"applied_maxmemory_bytes", result.AppliedMaxmemory,
		"target_maxmemory_mb", targetMaxmemoryMB,
		"effective_prid", effectivePRID,
		"skip_reason", result.SkipReason,
		"request_id", req.RequestId)

	// AppliedConnLimit is Postgres-specific; for Redis we repurpose it to carry
	// the applied maxmemory in MB so the worker can log it. The worker does not
	// store this value in a DB column — the reconciler is designed to be
	// stateless for Redis (it re-checks every tick, idempotently).
	appliedMB := int32(0)
	if result.Applied {
		appliedMB = int32(targetMaxmemoryMB)
	}
	return &provisionerv1.RegradeResponse{
		Applied:          result.Applied,
		AppliedConnLimit: appliedMB, // repurposed: maxmemory_mb for Redis
		SkipReason:       result.SkipReason,
	}, nil
}

// mapError converts backend errors to appropriate gRPC status codes.
//
// P1-02: when the error is a typed *pgconn.PgError, classification is driven by
// its SQLSTATE — NOT by substring-matching the free-text message. A real
// CREATE DATABASE failure (e.g. "permission denied", SQLSTATE 42501) whose
// message happens to contain the word "connection" must NOT be reported as
// Unavailable, or the worker retries a non-retryable failure. The substring
// path is kept only as a fallback for non-Postgres backends (redis/mongo/k8s)
// and for transport-layer errors that never reach a SQLSTATE.
func mapError(op string, err error) error {
	if err == nil {
		return nil
	}
	// Audit P0-3: a breaker-open response from any per-backend in-process
	// circuit MUST surface as gRPC Unavailable so the api caller can react
	// cleanly. Returning Internal would defeat the whole point of the
	// breaker — the api side would treat it as a non-retryable failure and
	// pass it through to the agent as a 500.
	if errors.Is(err, circuit.ErrOpen) {
		slog.Warn(fmt.Sprintf("server.%s", op), "circuit", "open", "error", err.Error())
		return status.Errorf(codes.Unavailable, "%s: provisioner circuit open: %v", op, err)
	}
	msg := err.Error()
	slog.Error(fmt.Sprintf("server.%s", op), "error", msg)

	// Typed Postgres error → classify on SQLSTATE, authoritatively.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case isPgConnectSQLSTATE(pgErr.Code):
			return status.Errorf(codes.Unavailable, "%s: connect failure: %v", op, err)
		case pgErr.Code == pgSQLStateDuplicateDatabase || pgErr.Code == pgSQLStateDuplicateObject:
			return status.Errorf(codes.AlreadyExists, "%s: already exists: %v", op, err)
		default:
			// Any other SQLSTATE (permission denied, syntax error, etc.) is a
			// real, non-retryable Internal failure — even if its message text
			// contains "connect"/"connection".
			return status.Errorf(codes.Internal, "%s: %v", op, err)
		}
	}

	// Non-Postgres backend (redis/mongo/k8s) or a transport error with no
	// SQLSTATE — fall back to message-substring heuristics.
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

// Postgres SQLSTATE codes used for typed error classification (see
// https://www.postgresql.org/docs/current/errcodes-appendix.html).
const (
	// pgSQLStateClassConnection is SQLSTATE class 08 — "Connection Exception".
	// Every code in this class (08000, 08003, 08006, 08001, 08004, 08007, 08P01)
	// is a genuine connect/transport failure and is safely retryable.
	pgSQLStateClassConnection = "08"
	// pgSQLStateCannotConnectNow is 57P03 — server is starting up / shutting
	// down. Transient and retryable, but not in class 08.
	pgSQLStateCannotConnectNow = "57P03"
	// pgSQLStateDuplicateDatabase is 42P04 — CREATE DATABASE of an existing DB.
	pgSQLStateDuplicateDatabase = "42P04"
	// pgSQLStateDuplicateObject is 42710 — e.g. CREATE USER of an existing role.
	pgSQLStateDuplicateObject = "42710"
)

// isPgConnectSQLSTATE reports whether a Postgres SQLSTATE denotes a genuine
// connect/transport failure that the caller may safely retry.
func isPgConnectSQLSTATE(code string) bool {
	return strings.HasPrefix(code, pgSQLStateClassConnection) || code == pgSQLStateCannotConnectNow
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
