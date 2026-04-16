package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryAuthInterceptor validates the x-instant-provisioner-token metadata on every
// incoming unary RPC. The request is rejected with Unauthenticated if the token is
// missing or does not match the shared secret.
func UnaryAuthInterceptor(secret string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		vals := md.Get("x-instant-provisioner-token")
		if len(vals) == 0 || vals[0] != secret {
			return nil, status.Error(codes.Unauthenticated, "invalid provisioner token")
		}
		return handler(ctx, req)
	}
}
