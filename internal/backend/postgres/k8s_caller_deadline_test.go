package postgres

// k8s_caller_deadline_test.go — regression guard for the pro-provision-hang bug
// class (mirrors redis #52, applied to postgres/mongo/queue on 2026-06-08).
//
// Before the fix the provisioning context derived from context.Background(), so
// when the api caller's gRPC deadline fired (or it cancelled the RPC) the
// provisioner kept blocking up to 5m on a wedged PVC/CSI attach and the api
// handler hung. The fix derives provCtx from the incoming ctx (with a 5m
// ceiling backstop), so a stalled provision fast-fails bounded by the caller's
// deadline and the shared mapError maps the ctx error to a retryable gRPC status.

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

// TestProvision_HonoursCallerDeadline: a Provision whose pod never becomes Ready
// must return promptly, bounded by the caller's deadline — NOT block for
// k8sReadyTimeout on a background clock.
func TestProvision_HonoursCallerDeadline(t *testing.T) {
	cs := fake.NewClientset() // empty cluster: the postgres pod never becomes Ready
	b := &K8sBackend{cs: cs, storageClass: "standard", image: "postgres:16"}

	// Caller deadline far shorter than k8sReadyTimeout and the 5m ceiling.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := b.Provision(ctx, "pro-token", "pro", 8)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Provision should fail when the postgres pod never becomes Ready; got nil error")
	}
	if elapsed > 30*time.Second {
		t.Fatalf("PROVISION-HANG REGRESSION: Provision took %s; it must honour the caller's "+
			"~300ms deadline and fast-fail, not block for k8sReadyTimeout. This means provCtx "+
			"no longer derives from the caller's ctx.", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Provision error should wrap context.DeadlineExceeded (got %v) so the shared "+
			"server.mapError surfaces a retryable gRPC status (api soft-deletes + 503s)", err)
	}
}
