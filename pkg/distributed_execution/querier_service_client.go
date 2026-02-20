package distributed_execution

import (
	"time"

	"github.com/go-kit/log"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributed_execution/querierpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	cortexmiddleware "github.com/cortexproject/cortex/pkg/util/middleware"
)

type querierClient struct {
	querierpb.QuerierClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (qc *querierClient) Close() error {
	return qc.conn.Close()
}

func NewQuerierPool(cfg grpcclient.Config, reg prometheus.Registerer, log log.Logger) *client.Pool {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_querier_query_request_duration_seconds",
		Help:    "Time spent doing requests to querier.",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 6),
	}, []string{"operation", "status_code"})

	clientsGauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace:   "cortex",
		Name:        "cortex_querier_query_clients",
		Help:        "TThe current number of clients connected to querier.",
		ConstLabels: map[string]string{"client": "querier"},
	})

	poolConfig := client.PoolConfig{
		CheckInterval:      time.Minute,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	q := &querierPool{
		grpcConfig:      cfg,
		requestDuration: requestDuration,
	}

	return client.NewPool("querier", poolConfig, nil, q.createQuerierClient, clientsGauge, log)
}

type querierPool struct {
	grpcConfig      grpcclient.Config
	requestDuration *prometheus.HistogramVec
}

func (q *querierPool) createQuerierClient(addr string) (client.PoolClient, error) {

	opts, err := q.grpcConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor,
		cortexmiddleware.PrometheusGRPCUnaryInstrumentation(q.requestDuration),
	}, []grpc.StreamClientInterceptor{
		otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
		middleware.StreamClientUserHeaderInterceptor,
		cortexmiddleware.PrometheusGRPCStreamInstrumentation(q.requestDuration),
	})

	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &querierClient{
		QuerierClient: querierpb.NewQuerierClient(conn),
		HealthClient:  grpc_health_v1.NewHealthClient(conn),
		conn:          conn,
	}, nil
}

func floatHistogramProtoToFloatHistograms(hps []cortexpb.Histogram) []*histogram.FloatHistogram {
	floatHistograms := make([]*histogram.FloatHistogram, len(hps))
	for _, hp := range hps {
		newHist := floatHistogramProtoToFloatHistogram(hp)
		floatHistograms = append(floatHistograms, newHist)
	}
	return floatHistograms
}

func floatHistogramProtoToFloatHistogram(hp cortexpb.Histogram) *histogram.FloatHistogram {
	_, IsFloatHist := hp.GetCount().(*cortexpb.Histogram_CountFloat)
	if !IsFloatHist {
		panic("FloatHistogramProtoToFloatHistogram called with an integer histogram")
	}
	return &histogram.FloatHistogram{
		CounterResetHint: histogram.CounterResetHint(hp.ResetHint),
		Schema:           hp.Schema,
		ZeroThreshold:    hp.ZeroThreshold,
		ZeroCount:        hp.GetZeroCountFloat(),
		Count:            hp.GetCountFloat(),
		Sum:              hp.Sum,
		PositiveSpans:    spansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets:  hp.GetPositiveCounts(),
		NegativeSpans:    spansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets:  hp.GetNegativeCounts(),
	}
}

func spansProtoToSpans(s []cortexpb.BucketSpan) []histogram.Span {
	spans := make([]histogram.Span, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = histogram.Span{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}
