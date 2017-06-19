package gcp

import (
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var bigtableRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "bigtable_request_duration_seconds",
	Help:      "Time spent doing BigTable requests.",

	// DynamoDB latency seems to range from a few ms to a few sec and is
	// important.  So use 8 buckets from 64us to 8s.
	Buckets: prometheus.ExponentialBuckets(0.000128, 4, 8),
}, []string{"operation", "status_code"})

func init() {
	prometheus.MustRegister(bigtableRequestDuration)
}

func instrumentation() option.ClientOption {
	return option.WithGRPCDialOption(
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			prometheusGRPCClientInstrumentation(bigtableRequestDuration),
		)),
	)
}

func prometheusGRPCClientInstrumentation(metric *prometheus.HistogramVec) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, resp interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		start := time.Now()
		err := invoker(ctx, method, req, resp, cc, opts...)
		metric.WithLabelValues(method, instrument.ErrorCode(err)).Observe(time.Now().Sub(start).Seconds())
		return err
	}
}
