package mongo

// k8s_caller_deadline_test.go — regression guard for the pro-provision-hang bug
// class (mirrors redis #52, applied to postgres/mongo/queue on 2026-06-08).
// See the postgres sibling file for the full rationale: provCtx now derives from
// the incoming ctx (with a 5m ceiling), so a stalled provision fast-fails on the
// caller's deadline instead of blocking up to 5m on a background clock.

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

// TestProvision_HonoursCallerDeadline: a Provision whose pod never becomes Ready
// must return promptly, bounded by the caller's deadline — NOT block for
// mongoK8sReadyTO on a background clock.
func TestProvision_HonoursCallerDeadline(t *testing.T) {
	cs := fake.NewClientset() // empty cluster: the mongo pod never becomes Ready
	b := &K8sBackend{cs: cs, storageClass: "standard", image: "mongo:7"}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := b.Provision(ctx, "pro-token", "pro")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Provision should fail when the mongo pod never becomes Ready; got nil error")
	}
	if elapsed > 30*time.Second {
		t.Fatalf("PROVISION-HANG REGRESSION: Provision took %s; it must honour the caller's "+
			"~300ms deadline and fast-fail, not block for mongoK8sReadyTO. This means provCtx "+
			"no longer derives from the caller's ctx.", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Provision error should wrap context.DeadlineExceeded (got %v) so the shared "+
			"server.mapError surfaces a retryable gRPC status (api soft-deletes + 503s)", err)
	}
}
