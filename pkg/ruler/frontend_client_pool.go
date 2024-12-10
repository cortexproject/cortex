package ruler

import (
	"time"

	"github.com/go-kit/log"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	cortexmiddleware "github.com/cortexproject/cortex/pkg/util/middleware"
)

type frontendPool struct {
	timeout              time.Duration
	queryResponseFormat  string
	prometheusHTTPPrefix string
	grpcConfig           grpcclient.Config

	frontendClientRequestDuration *prometheus.HistogramVec
}

func newFrontendPool(cfg Config, log log.Logger, reg prometheus.Registerer) *client.Pool {
	p := &frontendPool{
		timeout:              cfg.FrontendTimeout,
		queryResponseFormat:  cfg.QueryResponseFormat,
		prometheusHTTPPrefix: cfg.PrometheusHTTPPrefix,
		grpcConfig:           cfg.GRPCClientConfig,
		frontendClientRequestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_ruler_query_frontend_request_duration_seconds",
			Help:    "Time spend doing requests to frontend.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 6),
		}, []string{"operation", "status_code"}),
	}

	frontendClientsGauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ruler_query_frontend_clients",
		Help: "The current number of clients connected to query-frontend.",
	})

	poolConfig := client.PoolConfig{
		CheckInterval:      time.Minute,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	return client.NewPool("frontend", poolConfig, nil, p.createFrontendClient, frontendClientsGauge, log)
}

func (f *frontendPool) createFrontendClient(addr string) (client.PoolClient, error) {
	opts, err := f.grpcConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor,
		cortexmiddleware.PrometheusGRPCUnaryInstrumentation(f.frontendClientRequestDuration),
	}, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &frontendClient{
		FrontendClient: NewFrontendClient(httpgrpc.NewHTTPClient(conn), f.timeout, f.prometheusHTTPPrefix, f.queryResponseFormat),
		HealthClient:   grpc_health_v1.NewHealthClient(conn),
	}, nil
}

type frontendClient struct {
	*FrontendClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (f *frontendClient) Close() error {
	return f.conn.Close()
}
