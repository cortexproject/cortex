// Package fakeauth provides middlewares thats injects a fake userID, so the rest of the code
// can continue to be multitenant.
package fakeauth

import (
	"context"
	"net/http"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/user"
)

// SetupAuthMiddleware for the given server config.
func SetupAuthMiddleware(config *server.Config, enabled bool, propagator user.Propagator, noGRPCAuthOn []string) middleware.Interface {
	if enabled {
		config.GRPCMiddleware = append(config.GRPCMiddleware,
			middleware.WithPropagator(propagator).ServerUserHeaderInterceptor,
		)
		config.GRPCStreamMiddleware = append(config.GRPCStreamMiddleware,
			func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				for _, path := range noGRPCAuthOn {
					if info.FullMethod == path {
						return handler(srv, ss)
					}
				}
				return middleware.WithPropagator(propagator).StreamServerUserHeaderInterceptor(srv, ss, info, handler)
			},
		)
		return middleware.WithPropagator(propagator).AuthenticateUser()
	}

	config.GRPCMiddleware = append(config.GRPCMiddleware,
		fakeGRPCAuthUniaryMiddleware,
	)
	config.GRPCStreamMiddleware = append(config.GRPCStreamMiddleware,
		fakeGRPCAuthStreamMiddleware,
	)
	return fakeHTTPAuthMiddleware
}

var fakeHTTPAuthMiddleware = middleware.Func(func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := user.InjectTenantIDs(r.Context(), []string{"fake"})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
})

var fakeGRPCAuthUniaryMiddleware = func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = user.InjectTenantIDs(ctx, []string{"fake"})
	return handler(ctx, req)
}

var fakeGRPCAuthStreamMiddleware = func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := user.InjectTenantIDs(ss.Context(), []string{"fake"})
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
