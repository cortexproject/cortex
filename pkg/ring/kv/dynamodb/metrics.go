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
	dynamodbCasAttempts     prometheus.Counter
}

func newDynamoDbMetrics(registerer prometheus.Registerer) *dynamodbMetrics {
	dynamodbRequestDurationCollector := instrument.NewHistogramCollector(promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dynamodb_kv_request_duration_seconds",
		Help:    "Time spent on dynamodb requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "status_code"}))

	dynamodbUsageMetrics := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "dynamodb_kv_consumed_capacity_total",
		Help: "Total consumed capacity on dynamodb",
	}, []string{"operation"})

	dynamodbCasAttempts := promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Name: "dynamodb_kv_cas_attempt_total",
		Help: "DynamoDB KV Store Attempted CAS operations",
	})

	dynamodbMetrics := dynamodbMetrics{
		dynamodbRequestDuration: dynamodbRequestDurationCollector,
		dynamodbUsageMetrics:    dynamodbUsageMetrics,
		dynamodbCasAttempts:     dynamodbCasAttempts,
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
		totalCapacity, err := d.kv.Delete(ctx, key)
		d.ddbMetrics.dynamodbUsageMetrics.WithLabelValues("Delete").Add(totalCapacity)
		return err
	})
}

func (d dynamodbInstrumentation) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	return instrument.CollectedRequest(ctx, "Put", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		totalCapacity, err := d.kv.Put(ctx, key, data)
		d.ddbMetrics.dynamodbUsageMetrics.WithLabelValues("Put").Add(totalCapacity)
		return err
	})
}

func (d dynamodbInstrumentation) Batch(ctx context.Context, put map[dynamodbKey][]byte, delete []dynamodbKey) error {
	return instrument.CollectedRequest(ctx, "Batch", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		totalCapacity, err := d.kv.Batch(ctx, put, delete)
		d.ddbMetrics.dynamodbUsageMetrics.WithLabelValues("Batch").Add(totalCapacity)
		return err
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
