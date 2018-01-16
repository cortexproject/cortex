package client

import (
	"compress/gzip"
	"io"
	"sync"
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
func MakeIngesterClient(addr string, timeout time.Duration, withCompression bool) (IngesterClient, error) {
	opts := []grpc.DialOption{grpc.WithTimeout(timeout),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
	}
	if withCompression {
		opts = append(opts, grpc.WithCompressor(NewPooledGZIPCompressor()))
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

// NewPooledGZIPCompressor creates a Compressor based on GZIP.
// Based on the implementation in grpc library, but with a pool of
// objects to reduce garbage
func NewPooledGZIPCompressor() grpc.Compressor {
	return &pooledCompressor{
		pool: sync.Pool{New: func() interface{} { return gzip.NewWriter(nil) }},
	}
}

type pooledCompressor struct {
	pool sync.Pool
}

func (c *pooledCompressor) Do(w io.Writer, p []byte) error {
	z := c.pool.Get().(*gzip.Writer)
	defer c.pool.Put(z)
	z.Reset(w)
	if _, err := z.Write(p); err != nil {
		return err
	}
	return z.Close()
}

func (c *pooledCompressor) Type() string {
	return "gzip"
}
