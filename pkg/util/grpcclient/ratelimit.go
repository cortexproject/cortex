package grpcclient

import (
	"context"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

// NewRateLimiter creates a UnaryClientInterceptor for client side rate limiting.
func NewRateLimiter(r float64, b int) grpc.UnaryClientInterceptor {
	limiter := rate.NewLimiter(rate.Limit(r), b)
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		limiter.Wait(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
