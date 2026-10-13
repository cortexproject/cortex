// Package fakeauth provides middlewares that injects a fake userID, so the rest of the code
// can continue to be multitenant.
package fakeauth

import (
	"context"
	"net/http"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SetupAuthMiddleware for the given server config.
func SetupAuthMiddleware(config *server.Config, enabled bool, noGRPCAuthOn []string) middleware.Interface {
	if enabled {
		ignoredMethods := map[string]bool{}
		for _, m := range noGRPCAuthOn {
			ignoredMethods[m] = true
		}

		config.GRPCMiddleware = append(config.GRPCMiddleware, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
			if ignoredMethods[info.FullMethod] {
				return handler(ctx, req)
			}
			return ServerUserHeaderInterceptor(ctx, req, info, handler)
		})

		config.GRPCStreamMiddleware = append(config.GRPCStreamMiddleware,
			func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if ignoredMethods[info.FullMethod] {
					return handler(srv, ss)
				}
				return StreamServerUserHeaderInterceptor(srv, ss, info, handler)
			},
		)

		return middleware.AuthenticateUser
	}

	config.GRPCMiddleware = append(config.GRPCMiddleware,
		fakeGRPCAuthUniaryMiddleware,
	)
	config.GRPCStreamMiddleware = append(config.GRPCStreamMiddleware,
		fakeGRPCAuthStreamMiddleware,
	)
	return fakeHTTPAuthMiddleware
}

// ServerUserHeaderInterceptor propagates the tenant ID from the gRPC metadata to the context.
// Unlike middleware.ServerUserHeaderInterceptor, it rejects a request without a tenant ID with
// codes.Unauthenticated and not codes.Unknown, so that external gRPC clients (for example
// OTLP exporters) can see that the request failed permanently.
func ServerUserHeaderInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	_, ctx, err := user.ExtractFromGRPCRequest(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return handler(ctx, req)
}

// StreamServerUserHeaderInterceptor is the stream version of ServerUserHeaderInterceptor.
func StreamServerUserHeaderInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	_, ctx, err := user.ExtractFromGRPCRequest(ss.Context())
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	return handler(srv, serverStream{
		ctx:          ctx,
		ServerStream: ss,
	})
}

var fakeHTTPAuthMiddleware = middleware.Func(func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := user.InjectOrgID(r.Context(), "fake")
		next.ServeHTTP(w, r.WithContext(ctx))
	})
})

var fakeGRPCAuthUniaryMiddleware = func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx = user.InjectOrgID(ctx, "fake")
	return handler(ctx, req)
}

var fakeGRPCAuthStreamMiddleware = func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := user.InjectOrgID(ss.Context(), "fake")
	return handler(srv, serverStream{
		ctx:          ctx,
		ServerStream: ss,
	})
}

type serverStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss serverStream) Context() context.Context {
	return ss.ctx
}
