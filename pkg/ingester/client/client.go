package client

import (
	"context"
	"flag"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var ingesterClientRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "ingester_client_request_duration_seconds",
	Help:      "Time spent doing Ingester requests.",
	Buckets:   prometheus.ExponentialBuckets(0.001, 4, 6),
}, []string{"operation", "status_code"})
var inflightRequestCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "cortex",
	Name:      "ingester_client_request_count",
	Help:      "Number of Ingester client requests.",
}, []string{"ingester"})

var errTooManyInflightPushRequests = errors.New("too many inflight push requests in ingester client")

// ClosableClientConn is grpc.ClientConnInterface with Close function
type ClosableClientConn interface {
	grpc.ClientConnInterface
	Close() error
}

// HealthAndIngesterClient is the union of IngesterClient and grpc_health_v1.HealthClient.
type HealthAndIngesterClient interface {
	IngesterClient
	grpc_health_v1.HealthClient
	Close() error
	PushPreAlloc(ctx context.Context, in *cortexpb.PreallocWriteRequest, opts ...grpc.CallOption) (*cortexpb.WriteResponse, error)
}

type closableHealthAndIngesterClient struct {
	IngesterClient
	grpc_health_v1.HealthClient
	conn                    ClosableClientConn
	maxInflightPushRequests int64
	inflightRequests        atomic.Int64
	inflightRequestCount    prometheus.Gauge
}

func (c *closableHealthAndIngesterClient) PushPreAlloc(ctx context.Context, in *cortexpb.PreallocWriteRequest, opts ...grpc.CallOption) (*cortexpb.WriteResponse, error) {
	return c.handlePushRequest(func() (*cortexpb.WriteResponse, error) {
		out := new(cortexpb.WriteResponse)
		err := c.conn.Invoke(ctx, "/cortex.Ingester/Push", in, out, opts...)
		if err != nil {
			return nil, err
		}
		return out, nil
	})
}

func (c *closableHealthAndIngesterClient) Push(ctx context.Context, in *cortexpb.WriteRequest, opts ...grpc.CallOption) (*cortexpb.WriteResponse, error) {
	return c.handlePushRequest(func() (*cortexpb.WriteResponse, error) {
		return c.IngesterClient.Push(ctx, in, opts...)
	})
}

func (c *closableHealthAndIngesterClient) handlePushRequest(mainFunc func() (*cortexpb.WriteResponse, error)) (*cortexpb.WriteResponse, error) {
	currentInflight := c.inflightRequests.Inc()
	c.inflightRequestCount.Inc()
	defer func() {
		c.inflightRequestCount.Dec()
		c.inflightRequests.Dec()
	}()
	if c.maxInflightPushRequests > 0 && currentInflight > c.maxInflightPushRequests {
		return nil, errTooManyInflightPushRequests
	}
	return mainFunc()
}

// MakeIngesterClient makes a new IngesterClient
func MakeIngesterClient(addr string, cfg Config) (HealthAndIngesterClient, error) {
	dialOpts, err := cfg.GRPCClientConfig.DialOption(grpcclient.Instrument(ingesterClientRequestDuration))
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	return &closableHealthAndIngesterClient{
		IngesterClient:          NewIngesterClient(conn),
		HealthClient:            grpc_health_v1.NewHealthClient(conn),
		conn:                    conn,
		maxInflightPushRequests: cfg.MaxInflightPushRequests,
		inflightRequestCount:    inflightRequestCount.WithLabelValues(addr),
	}, nil
}

func (c *closableHealthAndIngesterClient) Close() error {
	return c.conn.Close()
}

// Config is the configuration struct for the ingester client
type Config struct {
	GRPCClientConfig        grpcclient.Config `yaml:"grpc_client_config"`
	MaxInflightPushRequests int64             `yaml:"max_inflight_push_requests"`
}

// RegisterFlags registers configuration settings used by the ingester client config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ingester.client", f)
	f.Int64Var(&cfg.MaxInflightPushRequests, "ingester.client.max-inflight-push-requests", 0, "Max inflight push requests that this ingester client can handle. This limit is per-ingester-client. Additional requests will be rejected. 0 = unlimited.")
}

func (cfg *Config) Validate(log log.Logger) error {
	return cfg.GRPCClientConfig.Validate(log)
}
