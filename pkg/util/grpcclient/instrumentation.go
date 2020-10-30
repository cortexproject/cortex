package grpcclient

import (
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/user"
	cortex_middleware "github.com/cortexproject/cortex/pkg/util/middleware"
)

func Instrument(requestDuration *prometheus.HistogramVec, propagator user.Propagator) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	return []grpc.UnaryClientInterceptor{
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.WithPropagator(propagator).ClientUserHeaderInterceptor,
			cortex_middleware.PrometheusGRPCUnaryInstrumentation(requestDuration),
		}, []grpc.StreamClientInterceptor{
			otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
			middleware.WithPropagator(propagator).StreamClientUserHeaderInterceptor,
			cortex_middleware.PrometheusGRPCStreamInstrumentation(requestDuration),
		}
}
