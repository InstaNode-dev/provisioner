package interceptor_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"instant.dev/provisioner/internal/interceptor"
)

const testSecret = "super-secret-token"

// fakeHandler is a gRPC handler that records whether it was called.
func fakeHandler(called *bool) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		*called = true
		return "ok", nil
	}
}

func TestUnaryAuthInterceptor_ValidSecret(t *testing.T) {
	inter := interceptor.UnaryAuthInterceptor(testSecret)
	md := metadata.Pairs("x-instant-provisioner-token", testSecret)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var called bool
	resp, err := inter(ctx, nil, nil, fakeHandler(&called))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %v", resp)
	}
}

func TestUnaryAuthInterceptor_WrongSecret(t *testing.T) {
	inter := interceptor.UnaryAuthInterceptor(testSecret)
	md := metadata.Pairs("x-instant-provisioner-token", "wrong-secret")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var called bool
	_, err := inter(ctx, nil, nil, fakeHandler(&called))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if called {
		t.Fatal("handler should not have been called")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestUnaryAuthInterceptor_MissingToken(t *testing.T) {
	inter := interceptor.UnaryAuthInterceptor(testSecret)
	// metadata present but no token key
	md := metadata.Pairs("other-header", "value")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var called bool
	_, err := inter(ctx, nil, nil, fakeHandler(&called))
	assertUnauthenticated(t, err, called)
}

func TestUnaryAuthInterceptor_NoMetadata(t *testing.T) {
	inter := interceptor.UnaryAuthInterceptor(testSecret)
	// No incoming metadata at all
	ctx := context.Background()

	var called bool
	_, err := inter(ctx, nil, nil, fakeHandler(&called))
	assertUnauthenticated(t, err, called)
}

// TestUnaryAuthInterceptor_EmptySecret_AlwaysRejects is the P1-M fail-closed
// regression guard. An interceptor constructed with an empty secret MUST reject
// EVERY RPC — an empty configured secret must never be interpreted as "auth
// disabled". Before this fix an empty token (vals[0] == "") matched an empty
// secret ("") via `==` and the RPC was served unauthenticated.
func TestUnaryAuthInterceptor_EmptySecret_AlwaysRejects(t *testing.T) {
	inter := interceptor.UnaryAuthInterceptor("")

	// Case 1: the old bypass — empty token vs empty secret.
	t.Run("empty token does not bypass", func(t *testing.T) {
		md := metadata.Pairs("x-instant-provisioner-token", "")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		var called bool
		_, err := inter(ctx, nil, nil, fakeHandler(&called))
		assertUnauthenticated(t, err, called)
	})

	// Case 2: any token at all is rejected when the server is misconfigured.
	t.Run("non-empty token still rejected", func(t *testing.T) {
		md := metadata.Pairs("x-instant-provisioner-token", "anything")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		var called bool
		_, err := inter(ctx, nil, nil, fakeHandler(&called))
		assertUnauthenticated(t, err, called)
	})

	// Case 3: no metadata at all is rejected when the server is misconfigured.
	t.Run("no metadata still rejected", func(t *testing.T) {
		var called bool
		_, err := inter(context.Background(), nil, nil, fakeHandler(&called))
		assertUnauthenticated(t, err, called)
	})
}

// TestValidateSecret is the P1-M startup fail-closed guard. main.go calls
// ValidateSecret and aborts on a non-nil error so the provisioner refuses to
// boot with auth disabled.
func TestValidateSecret(t *testing.T) {
	if err := interceptor.ValidateSecret(""); err == nil {
		t.Fatal("ValidateSecret(\"\") must return an error — empty PROVISIONER_SECRET must fail closed")
	}
	if err := interceptor.ValidateSecret("super-secret-token"); err != nil {
		t.Fatalf("ValidateSecret with a non-empty secret should succeed, got %v", err)
	}
}

func assertUnauthenticated(t *testing.T, err error, handlerCalled bool) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if handlerCalled {
		t.Fatal("handler should not have been called")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", st.Code())
	}
}
