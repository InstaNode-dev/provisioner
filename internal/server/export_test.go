package server

import (
	"fmt"

	"instant.dev/common/plans"

	"instant.dev/provisioner/internal/backend/redis"
)

// SwapRegradeConnLimits temporarily replaces the package-level plans registry
// used by RegradeResource and returns a restore func. It exists so external
// tests can drive the "no cap" sentinel branch in regradeRedis (a tier whose
// redis_memory_mb resolves to <= 0), which no current production tier triggers
// post the strict-80 margin redesign — the branch is kept for the wire
// contract, so it needs an explicit registry-seam test rather than a live tier.
func SwapRegradeConnLimits(r *plans.Registry) (restore func()) {
	prev := regradeConnLimits
	regradeConnLimits = r
	return func() { regradeConnLimits = prev }
}

// RedisBackendIsTierDispatch reports whether the server's shared redis backend
// is wrapped in a tier-aware dispatcher. It lets external tests assert the
// REDIS_TIER_AWARE_ROUTING_ENABLED flag wiring in New() without exporting the
// unexported redisBackend field in production code.
func RedisBackendIsTierDispatch(s *Server) bool {
	_, ok := s.redisBackend.(*redis.TierDispatchBackend)
	return ok
}

// RedisBackendTypeName returns the concrete type of the server's shared redis
// backend, for diagnostic assertions in tests.
func RedisBackendTypeName(s *Server) string {
	return fmt.Sprintf("%T", s.redisBackend)
}
