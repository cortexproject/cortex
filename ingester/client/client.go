package client

import (
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/cortex"
)

type ingesterClient struct {
	cortex.IngesterClient
	conn *grpc.ClientConn
}

// MakeIngesterClient makes a new cortex.IngesterClient
func MakeIngesterClient(addr string, timeout time.Duration) (cortex.IngesterClient, error) {
	conn, err := grpc.Dial(
		addr,
		grpc.WithTimeout(timeout),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
	)
	if err != nil {
		return nil, err
	}
	return &ingesterClient{
		IngesterClient: cortex.NewIngesterClient(conn),
		conn:           conn,
	}, nil
}

func (c *ingesterClient) Close() error {
	return c.conn.Close()
}
