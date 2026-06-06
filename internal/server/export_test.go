package server

import "instant.dev/common/plans"

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
