package correctness

import (
	"context"
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

const (
	namespace = "prometheus"
	subsystem = "test_exporter"
)

type simpleTestCase struct {
	prometheus.GaugeFunc
	name            string
	expectedValueAt func(time.Time) float64
}

// NewSimpleTestCase makes a new simpleTestCase
func NewSimpleTestCase(name string, f func(time.Time) float64) Case {
	return &simpleTestCase{
		GaugeFunc: prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      name,
				Help:      name,
			},
			func() float64 {
				return f(time.Now())
			},
		),
		name:            name,
		expectedValueAt: f,
	}
}

func (tc *simpleTestCase) Name() string {
	return tc.name
}

func (tc *simpleTestCase) ExpectedValueAt(t time.Time) float64 {
	return tc.expectedValueAt(t)
}

func (tc *simpleTestCase) Query(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) ([]model.SamplePair, error) {
	log, ctx := spanlogger.New(ctx, "simpleTestCase.Query")
	defer log.Finish()

	metricName := prometheus.BuildFQName(namespace, subsystem, tc.name)
	query := fmt.Sprintf("%s{%s}[%dm]", metricName, selectors, duration/time.Minute)
	level.Info(log).Log("query", query)

	value, wrngs, err := client.Query(ctx, query, start)
	if err != nil {
		return nil, err
	}
	if wrngs != nil {
		level.Warn(log).Log(
			"query", query,
			"start", start,
			"warnings", wrngs,
		)
	}
	if value.Type() != model.ValMatrix {
		return nil, fmt.Errorf("didn't get matrix from Prom")
	}

	ms, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("didn't get matrix from Prom")
	}

	var result []model.SamplePair
	for _, stream := range ms {
		result = append(result, stream.Values...)
	}
	return result, nil
}

func (tc *simpleTestCase) Quantized(duration time.Duration) time.Duration {
	return duration.Truncate(time.Minute)
}
