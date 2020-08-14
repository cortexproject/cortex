package stats

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

func NewStatsHandler(r prometheus.Registerer) stats.Handler {
	// We don’t particularly care about small requests / responses,
	// we want to know more about the big ones.
	// Histogram goes linearly from 30MB to 210MB in 8 buckets.
	messageSizeBuckets := prometheus.LinearBuckets(30_000_000, 30_000_000, 8)

	return &grpcStatsHandler{
		connectedClients: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_grpc_connected_clients",
			Help: "Number of clients connected to gRPC server",
		}),

		inflightRpc: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_grpc_inflight_requests",
			Help: "Number of inflight RPC calls",
		}, []string{"method"}),

		receivedMessageSize: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_grpc_request_size_bytes",
			Help:    "Size of gRPC requests.",
			Buckets: messageSizeBuckets,
		}, []string{"method"}),

		sentMessageSize: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_grpc_response_size_bytes",
			Help:    "Size of gRPC responses.",
			Buckets: messageSizeBuckets,
		}, []string{"method"}),
	}
}

type grpcStatsHandler struct {
	connectedClients    prometheus.Gauge
	inflightRpc         *prometheus.GaugeVec
	receivedMessageSize *prometheus.HistogramVec
	sentMessageSize     *prometheus.HistogramVec
}

// Custom type to hide it from other packages.
type contextKey int

const (
	contextKeyMethodName contextKey = 1
)

func (g *grpcStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, contextKeyMethodName, info.FullMethodName)
}

func (g *grpcStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	// We use full method name from context, because not all RPCStats structs have it.
	fullMethodName, ok := ctx.Value(contextKeyMethodName).(string)
	if !ok {
		return
	}

	switch s := rpcStats.(type) {
	case *stats.Begin:
		g.inflightRpc.WithLabelValues(fullMethodName).Inc()
	case *stats.End:
		g.inflightRpc.WithLabelValues(fullMethodName).Dec()

	case *stats.InHeader:
		g.receivedMessageSize.WithLabelValues(fullMethodName).Observe(float64(s.WireLength))
	case *stats.InPayload:
		g.receivedMessageSize.WithLabelValues(fullMethodName).Observe(float64(s.WireLength))
	case *stats.InTrailer:
		g.receivedMessageSize.WithLabelValues(fullMethodName).Observe(float64(s.WireLength))

	case *stats.OutHeader:
		// OutHeader doesn't have WireLength.
		g.sentMessageSize.WithLabelValues(fullMethodName).Observe(estimateSize(s.Header))
	case *stats.OutPayload:
		g.sentMessageSize.WithLabelValues(fullMethodName).Observe(float64(s.WireLength))
	case *stats.OutTrailer:
		// OutTrailer doesn't have valid WireLength (there is deperecated field, always set to 0).
		g.sentMessageSize.WithLabelValues(fullMethodName).Observe(estimateSize(s.Trailer))
	}
}

// This returns estimate for message size for encoding metadata.
// Doesn't take any possible compression into account.
func estimateSize(md metadata.MD) float64 {
	result := 0
	for k, vs := range md {
		result += len(k)
		for _, v := range vs {
			result += len(v)
		}
	}
	return float64(result)
}

func (g *grpcStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (g *grpcStatsHandler) HandleConn(_ context.Context, connStats stats.ConnStats) {
	switch connStats.(type) {
	case *stats.ConnBegin:
		g.connectedClients.Inc()

	case *stats.ConnEnd:
		g.connectedClients.Inc()
	}
}
