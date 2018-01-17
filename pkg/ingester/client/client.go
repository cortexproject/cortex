package client

import (
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // get gzip compressor registered

	"github.com/weaveworks/common/middleware"
)

type closableIngesterClient struct {
	IngesterClient
	conn *grpc.ClientConn
}

// MakeIngesterClient makes a new IngesterClient
func MakeIngesterClient(addr string, timeout time.Duration, withCompression bool) (IngesterClient, error) {
	opts := []grpc.DialOption{grpc.WithTimeout(timeout),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
		// We have seen 20MB returns from queries - add a bit of headroom
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)),
	}
	if withCompression {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &closableIngesterClient{
		IngesterClient: NewIngesterClient(conn),
		conn:           conn,
	}, nil
}

func (c *closableIngesterClient) Close() error {
	return c.conn.Close()
}
