package aws

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	promApi "github.com/prometheus/client_golang/api"
	promV1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/mtime"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	cachePromDataFor       = 30 * time.Second
	queueObservationPeriod = 2 * time.Minute
	queueLengthScaledown   = 10000   // consider scaling down if queue smaller than this
	queueLengthAcceptable  = 100000  // we don't mind queues smaller than this
	queueLengthMax         = 1000000 // always scale up if queue bigger than this
	errorFractionScaledown = 0.1
	scaledown              = 0.9
	scaleup                = 1.2
)

type metricsData struct {
	promAPI      promV1.API
	lastUpdated  time.Time
	queueLengths []float64
	errorRates   map[string]float64
}

func (d dynamoTableClient) metricsAutoScale(ctx context.Context, current, expected *chunk.TableDesc) error {
	m := d.metrics
	if err := m.update(ctx); err != nil {
		return err
	}

	errorRate := m.errorRates[expected.Name]

	level.Info(util.Logger).Log("msg", "checking metrics", "table", current.Name, "queueLengths", fmt.Sprint(m.queueLengths), "errorRate", errorRate)

	switch {
	case m.queueLengths[2] < queueLengthScaledown && errorRate < errorFractionScaledown*float64(current.ProvisionedWrite):
		// No big queue, low errors -> scale down
		expected.ProvisionedWrite = int64(float64(current.ProvisionedWrite) * scaledown)
		level.Info(util.Logger).Log("msg", "metrics scale-down", "table", current.Name, "write", expected.ProvisionedWrite)
	case errorRate > 0 && m.queueLengths[2] > queueLengthMax:
		// Too big queue, some errors -> scale up
		expected.ProvisionedWrite = int64(float64(current.ProvisionedWrite) * scaleup)
		level.Info(util.Logger).Log("msg", "metrics max queue scale-up", "table", current.Name, "write", expected.ProvisionedWrite)
	case errorRate > 0 &&
		m.queueLengths[2] > queueLengthAcceptable &&
		m.queueLengths[2] > m.queueLengths[1] && m.queueLengths[1] > m.queueLengths[0]:
		// Growing queue, some errors -> scale up
		expected.ProvisionedWrite = int64(float64(current.ProvisionedWrite) * scaleup)
		level.Info(util.Logger).Log("msg", "metrics queue growing scale-up", "table", current.Name, "write", expected.ProvisionedWrite)
	default:
		// Nothing much happening - set expected to current
		expected.ProvisionedWrite = current.ProvisionedWrite
	}
	return nil
}

func newMetrics(cfg DynamoDBConfig) (*metricsData, error) {
	var promAPI promV1.API
	if cfg.MetricsURL != "" {
		client, err := promApi.NewClient(promApi.Config{Address: cfg.MetricsURL})
		if err != nil {
			return nil, err
		}
		promAPI = promV1.NewAPI(client)
	}
	return &metricsData{promAPI: promAPI}, nil
}

func (m *metricsData) update(ctx context.Context) error {
	if m.lastUpdated.After(mtime.Now().Add(-cachePromDataFor)) {
		return nil
	}

	qlMatrix, err := promQuery(ctx, m.promAPI, `sum(cortex_ingester_flush_queue_length)`, queueObservationPeriod, queueObservationPeriod/2)
	if err != nil {
		return err
	}
	if len(qlMatrix) != 1 {
		return errors.Errorf("expected one sample stream for queue: %d", len(qlMatrix))
	}
	if len(qlMatrix[0].Values) != 3 {
		return errors.Errorf("expected three values: %d", len(qlMatrix[0].Values))
	}
	m.queueLengths = make([]float64, len(qlMatrix[0].Values))
	for i, v := range qlMatrix[0].Values {
		m.queueLengths[i] = float64(v.Value)
	}

	deMatrix, err := promQuery(ctx, m.promAPI, `sum(rate(cortex_dynamo_failures_total{error="ProvisionedThroughputExceededException",operation=~".*Write.*"}[1m])) by (table) > 0`, 0, time.Second)
	if err != nil {
		return err
	}
	m.errorRates = make(map[string]float64)
	for _, s := range deMatrix {
		table, found := s.Metric["table"]
		if !found {
			continue
		}
		if len(s.Values) != 1 {
			return errors.Errorf("expected one sample for table %s: %d", table, len(s.Values))
		}
		m.errorRates[string(table)] = float64(s.Values[0].Value)
	}

	m.lastUpdated = mtime.Now()
	return nil
}

func promQuery(ctx context.Context, promAPI promV1.API, query string, duration, step time.Duration) (model.Matrix, error) {
	queryRange := promV1.Range{
		Start: mtime.Now().Add(-duration),
		End:   mtime.Now(),
		Step:  step,
	}

	value, err := promAPI.QueryRange(ctx, query, queryRange)
	if err != nil {
		return nil, err
	}
	matrix, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("Unable to convert value to matrix: %#v", value)
	}
	return matrix, nil
}
