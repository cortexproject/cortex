package client

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/grpcencoding/snappyblock"
	"github.com/weaveworks/common/user"

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
	Buckets:   prometheus.ExponentialBuckets(0.001, 4, 7),
}, []string{"operation", "status_code"})
var ingesterClientInflightPushRequests = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "cortex",
	Name:      "ingester_client_inflight_push_requests",
	Help:      "Number of Ingester client push requests.",
}, []string{"ingester"})

var errTooManyInflightPushRequests = errors.New("too many inflight push requests in ingester client")

const INGESTER_CLIENT_STREAM_WORKER_COUNT = 1024

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
	PushStreamConnection(ctx context.Context, in *cortexpb.WriteRequest, opts ...grpc.CallOption) (*cortexpb.WriteResponse, error)
}

type streamWriteJob struct {
	req    *cortexpb.StreamWriteRequest
	resp   *cortexpb.WriteResponse
	ctx    context.Context
	cancel context.CancelFunc
	err    error
}

type closableHealthAndIngesterClient struct {
	IngesterClient
	grpc_health_v1.HealthClient
	conn                    ClosableClientConn
	addr                    string
	maxInflightPushRequests int64
	inflightRequests        atomic.Int64
	inflightPushRequests    *prometheus.GaugeVec
	streamPushChan          chan *streamWriteJob
	streamCtx               context.Context
	streamCancel            context.CancelFunc
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

func (c *closableHealthAndIngesterClient) PushStreamConnection(ctx context.Context, in *cortexpb.WriteRequest, opts ...grpc.CallOption) (*cortexpb.WriteResponse, error) {
	return c.handlePushRequest(func() (*cortexpb.WriteResponse, error) {
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		streamReq := &cortexpb.StreamWriteRequest{
			TenantID: tenantID,
			Request:  in,
		}

		reqCtx, reqCancel := context.WithCancel(ctx)
		defer reqCancel()

		job := &streamWriteJob{
			req:    streamReq,
			ctx:    reqCtx,
			cancel: reqCancel,
		}
		c.streamPushChan <- job
		for {
			select {
			case <-reqCtx.Done():
				return job.resp, job.err
			}
		}
	})
}

func (c *closableHealthAndIngesterClient) handlePushRequest(mainFunc func() (*cortexpb.WriteResponse, error)) (*cortexpb.WriteResponse, error) {
	currentInflight := c.inflightRequests.Inc()
	c.inflightPushRequests.WithLabelValues(c.addr).Set(float64(currentInflight))
	defer func() {
		c.inflightPushRequests.WithLabelValues(c.addr).Set(float64(c.inflightRequests.Dec()))
	}()
	if c.maxInflightPushRequests > 0 && currentInflight > c.maxInflightPushRequests {
		return nil, errTooManyInflightPushRequests
	}
	return mainFunc()
}

// MakeIngesterClient makes a new IngesterClient
func MakeIngesterClient(addr string, cfg Config, useStreamConnection bool) (HealthAndIngesterClient, error) {
	dialOpts, err := cfg.GRPCClientConfig.DialOption(grpcclient.Instrument(ingesterClientRequestDuration))
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	c := &closableHealthAndIngesterClient{
		IngesterClient:          NewIngesterClient(conn),
		HealthClient:            grpc_health_v1.NewHealthClient(conn),
		conn:                    conn,
		addr:                    addr,
		maxInflightPushRequests: cfg.MaxInflightPushRequests,
		inflightPushRequests:    ingesterClientInflightPushRequests,
		streamPushChan:          make(chan *streamWriteJob, INGESTER_CLIENT_STREAM_WORKER_COUNT),
		streamCtx:               streamCtx,
		streamCancel:            streamCancel,
	}
	if useStreamConnection {
		err = c.Run()
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *closableHealthAndIngesterClient) Close() error {
	c.inflightPushRequests.DeleteLabelValues(c.addr)
	c.streamCancel()
	return c.conn.Close()
}

func (c *closableHealthAndIngesterClient) Run() error {
	var err error
	for i := 0; i < INGESTER_CLIENT_STREAM_WORKER_COUNT; i++ {
		workerCtx := user.InjectOrgID(c.streamCtx, fmt.Sprintf("stream-worker-%d", i))
		go func() {
			for {
				select {
				case <-workerCtx.Done():
					return
				default:
					err = c.worker(workerCtx)
					if err != nil {
						return
					}
				}
			}
		}()
	}
	return err
}

func (c *closableHealthAndIngesterClient) worker(ctx context.Context) error {
	stream, err := c.PushStream(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case job := <-c.streamPushChan:
			err = stream.Send(job.req)
			if err == io.EOF {
				job.resp = &cortexpb.WriteResponse{}
				job.cancel()
				return nil
			}
			if err != nil {
				job.err = err
				job.cancel()
				continue
			}
			resp, err := stream.Recv()
			if err == io.EOF {
				job.resp = &cortexpb.WriteResponse{}
				job.cancel()
				return nil
			}
			job.resp = resp
			job.err = err
			job.cancel()
		}
	}
}

// Config is the configuration struct for the ingester client
type Config struct {
	GRPCClientConfig        grpcclient.ConfigWithHealthCheck `yaml:"grpc_client_config"`
	MaxInflightPushRequests int64                            `yaml:"max_inflight_push_requests"`
}

// RegisterFlags registers configuration settings used by the ingester client config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ingester.client", snappyblock.Name, f)
	f.Int64Var(&cfg.MaxInflightPushRequests, "ingester.client.max-inflight-push-requests", 0, "Max inflight push requests that this ingester client can handle. This limit is per-ingester-client. Additional requests will be rejected. 0 = unlimited.")
}

func (cfg *Config) Validate(log log.Logger) error {
	return cfg.GRPCClientConfig.Validate(log)
}
