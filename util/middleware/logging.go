package middleware

import (
	"time"

	"github.com/prometheus/common/log"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const gRPC = "gRPC"

// ServerLoggingInterceptor logs gRPC requests, errors and latency.
func ServerLoggingInterceptor(logSuccess bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		begin := time.Now()
		resp, err := handler(ctx, req)
		if logSuccess || err != nil {
			log.Infof("%s %s (%v) %s", gRPC, info.FullMethod, err, time.Since(begin))
		}
		return resp, err
	}
}
