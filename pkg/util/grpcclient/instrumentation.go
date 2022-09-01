package grpcclient

import (
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util/grpcutil"
	cortexmiddleware "github.com/cortexproject/cortex/pkg/util/middleware"
)

func Instrument(requestDuration *prometheus.HistogramVec) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	return []grpc.UnaryClientInterceptor{
			grpcutil.HTTPHeaderPropagationClientInterceptor,
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
			cortexmiddleware.PrometheusGRPCUnaryInstrumentation(requestDuration),
		}, []grpc.StreamClientInterceptor{
			grpcutil.HTTPHeaderPropagationStreamClientInterceptor,
			otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
			middleware.StreamClientUserHeaderInterceptor,
			cortexmiddleware.PrometheusGRPCStreamInstrumentation(requestDuration),
		}
}
