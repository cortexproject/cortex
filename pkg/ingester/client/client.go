package client

import (
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // get gzip compressor registered

	"github.com/weaveworks/common/middleware"
	"flag"
)

type closableIngesterClient struct {
	IngesterClient
	conn *grpc.ClientConn
	cfg  Config
}

// MakeIngesterClient makes a new IngesterClient
func MakeIngesterClient(addr string, cfg Config) (IngesterClient, error) {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize)),
	}
	if cfg.CompressToIngester {
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

type Config struct {
	MaxRecvMsgSize int
	CompressToIngester bool
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	// We have seen 20MB returns from queries - add a bit of headroom
	f.IntVar(&cfg.MaxRecvMsgSize, "ingester.client.max-recv-message-size", 64*1024*1024, "Maximum message size, in bytes, this client will receive.")
	// moved from distributor pkg, but flag prefix left as-is so existing users do not break.
	flag.BoolVar(&cfg.CompressToIngester, "distributor.compress-to-ingester", false, "Compress data in calls to ingesters.")
}
