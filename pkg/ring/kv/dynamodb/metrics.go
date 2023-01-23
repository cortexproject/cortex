package dynamodb

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	grpcUtils "github.com/weaveworks/common/grpc"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/instrument"
)

type dynamodbInstrumentation struct {
	kv         dynamodbKV
	ddbMetrics *dynamodbMetrics
}

type dynamodbMetrics struct {
	dynamodbRequestDuration *instrument.HistogramCollector
	dynamodbUsageMetrics    *prometheus.CounterVec
}

func newDynamoDbMetrics(registerer prometheus.Registerer) *dynamodbMetrics {
	dynamodbRequestDurationCollector := instrument.NewHistogramCollector(promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dynamodb_kv_request_duration_seconds",
		Help:    "Time spent on dynamodb requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "status_code"}))

	dynamodbUsageMetrics := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "dynamodb_kv_read_capacity_total",
		Help: "Total used read capacity on dynamodb",
	}, []string{"operation"})

	dynamodbMetrics := dynamodbMetrics{
		dynamodbRequestDuration: dynamodbRequestDurationCollector,
		dynamodbUsageMetrics:    dynamodbUsageMetrics,
	}
	return &dynamodbMetrics
}

func (d dynamodbInstrumentation) List(ctx context.Context, key dynamodbKey) ([]string, float64, error) {
	var resp []string
	var totalCapacity float64
	err := instrument.CollectedRequest(ctx, "List", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		var err error
		resp, totalCapacity, err = d.kv.List(ctx, key)
		return err
	})
	d.ddbMetrics.dynamodbUsageMetrics.WithLabelValues("List").Add(totalCapacity)
	return resp, totalCapacity, err
}

func (d dynamodbInstrumentation) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, float64, error) {
	var resp map[string][]byte
	var totalCapacity float64
	err := instrument.CollectedRequest(ctx, "Query", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		var err error
		resp, totalCapacity, err = d.kv.Query(ctx, key, isPrefix)
		return err
	})
	d.ddbMetrics.dynamodbUsageMetrics.WithLabelValues("Query").Add(totalCapacity)
	return resp, totalCapacity, err
}

func (d dynamodbInstrumentation) Delete(ctx context.Context, key dynamodbKey) error {
	return instrument.CollectedRequest(ctx, "Delete", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		return d.kv.Delete(ctx, key)
	})
}

func (d dynamodbInstrumentation) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	return instrument.CollectedRequest(ctx, "Put", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		return d.kv.Put(ctx, key, data)
	})
}

func (d dynamodbInstrumentation) Batch(ctx context.Context, put map[dynamodbKey][]byte, delete []dynamodbKey) error {
	return instrument.CollectedRequest(ctx, "Batch", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		return d.kv.Batch(ctx, put, delete)
	})
}

// errorCode converts an error into an error code string.
func errorCode(err error) string {
	if err == nil {
		return "2xx"
	}

	if errResp, ok := httpgrpc.HTTPResponseFromError(err); ok {
		statusFamily := int(errResp.Code / 100)
		return strconv.Itoa(statusFamily) + "xx"
	} else if grpcUtils.IsCanceled(err) {
		return "cancel"
	} else {
		return "error"
	}
}
