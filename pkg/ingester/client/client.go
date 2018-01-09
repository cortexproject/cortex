package client

import (
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
)

type closableIngesterClient struct {
	IngesterClient
	conn *grpc.ClientConn
}

// MakeIngesterClient makes a new IngesterClient
func MakeIngesterClient(addr string, timeout time.Duration) (IngesterClient, error) {
	opts := []grpc.DialOption{grpc.WithTimeout(timeout),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
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
