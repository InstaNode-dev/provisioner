package interceptor

import (
	"context"
	"crypto/subtle"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// authMetadataKey is the gRPC metadata key carrying the shared provisioner
	// secret on every incoming RPC.
	authMetadataKey = "x-instant-provisioner-token"

	// errMsgMissingMetadata / errMsgInvalidToken / errMsgServerMisconfigured are
	// the Unauthenticated status messages returned for each rejection path.
	errMsgMissingMetadata     = "missing metadata"
	errMsgInvalidToken        = "invalid provisioner token"
	errMsgServerMisconfigured = "provisioner auth not configured"
)

// ErrEmptySecret is returned by ValidateSecret when the configured shared
// secret is empty. main.go uses it to FAIL CLOSED at startup — the provisioner
// must refuse to boot rather than silently serve unauthenticated RPCs.
var ErrEmptySecret = errors.New("interceptor: PROVISIONER_SECRET is empty — refusing to start with auth disabled")

// ValidateSecret reports whether the configured shared secret is usable.
// An empty secret is rejected: an unauthenticated gRPC provisioner is a
// remote-code-execution surface (it creates/destroys real databases). Callers
// (main.go) must treat a non-nil error as fatal and abort startup.
func ValidateSecret(secret string) error {
	if secret == "" {
		return ErrEmptySecret
	}
	return nil
}

// UnaryAuthInterceptor validates the x-instant-provisioner-token metadata on every
// incoming unary RPC. The request is rejected with Unauthenticated if the token is
// missing or does not match the shared secret.
//
// FAIL-CLOSED contract (P1-M, 2026-05-17): when the interceptor is constructed
// with an empty secret it rejects EVERY RPC with codes.Unauthenticated. An empty
// configured secret must never be interpreted as "auth disabled" — that would
// silently expose database create/destroy RPCs to any caller. main.go should
// additionally call ValidateSecret and refuse to start, so this is defence in
// depth: even if a future caller skips that check, no RPC is ever served
// unauthenticated.
//
// The provided token is compared to the configured secret with
// subtle.ConstantTimeCompare so the comparison does not leak the secret's
// length or contents through a timing side-channel.
func UnaryAuthInterceptor(secret string) grpc.UnaryServerInterceptor {
	// Computed once at construction — an empty secret is a hard misconfiguration.
	secretConfigured := secret != ""
	secretBytes := []byte(secret)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Fail closed: an empty configured secret rejects all RPCs. This must
		// run BEFORE the metadata check so a misconfigured server can never be
		// reached by any caller, regardless of what metadata they send.
		if !secretConfigured {
			return nil, status.Error(codes.Unauthenticated, errMsgServerMisconfigured)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, errMsgMissingMetadata)
		}
		vals := md.Get(authMetadataKey)
		// Constant-time compare: subtle.ConstantTimeCompare returns 1 only when
		// both byte slices are equal in length and content. A missing token
		// (len(vals) == 0) compares an empty slice and never matches a non-empty
		// secret.
		if len(vals) == 0 || subtle.ConstantTimeCompare([]byte(vals[0]), secretBytes) != 1 {
			return nil, status.Error(codes.Unauthenticated, errMsgInvalidToken)
		}
		return handler(ctx, req)
	}
}
