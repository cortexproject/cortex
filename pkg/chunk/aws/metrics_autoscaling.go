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
	targetScaledown        = 0.1 // consider scaling down if queue smaller than this times target
	targetMax              = 10  // always scale up if queue bigger than this times target
	errorFractionScaledown = 0.1
	scaledown              = 0.9
	scaleup                = 1.2
)

type metricsData struct {
	queueLengthTarget int64
	promAPI           promV1.API
	lastUpdated       time.Time
	queueLengths      []float64
	errorRates        map[string]float64
}

func (d dynamoTableClient) metricsAutoScale(ctx context.Context, current, expected *chunk.TableDesc) error {
	m := d.metrics
	if err := m.update(ctx); err != nil {
		return err
	}

	errorRate := m.errorRates[expected.Name]

	level.Info(util.Logger).Log("msg", "checking metrics", "table", current.Name, "queueLengths", fmt.Sprint(m.queueLengths), "errorRate", errorRate)

	// If we don't take explicit action, return the current provision as the expected provision
	expected.ProvisionedWrite = current.ProvisionedWrite

	switch {
	case errorRate < errorFractionScaledown*float64(current.ProvisionedWrite) &&
		m.queueLengths[2] < float64(m.queueLengthTarget)*targetScaledown:
		// No big queue, low errors -> scale down
		scaleDownWrite(current, expected, int64(float64(current.ProvisionedWrite)*scaledown), "metrics scale-down")
	case errorRate > 0 && m.queueLengths[2] > float64(m.queueLengthTarget)*targetMax:
		// Too big queue, some errors -> scale up
		scaleUpWrite(current, expected, int64(float64(current.ProvisionedWrite)*scaleup), "metrics max queue scale-up")
	case errorRate > 0 &&
		m.queueLengths[2] > float64(m.queueLengthTarget) &&
		m.queueLengths[2] > m.queueLengths[1] && m.queueLengths[1] > m.queueLengths[0]:
		// Growing queue, some errors -> scale up
		scaleUpWrite(current, expected, int64(float64(current.ProvisionedWrite)*scaleup), "metrics queue growing scale-up")
	}
	return nil
}

func scaleDownWrite(current, expected *chunk.TableDesc, newWrite int64, msg string) {
	if newWrite < expected.WriteScale.MinCapacity {
		newWrite = expected.WriteScale.MinCapacity
	}
	if newWrite < current.ProvisionedWrite {
		level.Info(util.Logger).Log("msg", msg, "table", current.Name, "write", newWrite)
		expected.ProvisionedWrite = newWrite
	}
}

func scaleUpWrite(current, expected *chunk.TableDesc, newWrite int64, msg string) {
	if newWrite > expected.WriteScale.MaxCapacity {
		newWrite = expected.WriteScale.MaxCapacity
	}
	if newWrite > current.ProvisionedWrite {
		level.Info(util.Logger).Log("msg", msg, "table", current.Name, "write", newWrite)
		expected.ProvisionedWrite = newWrite
	}
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
	return &metricsData{
		promAPI:           promAPI,
		queueLengthTarget: cfg.MetricsTargetQueueLen,
	}, nil
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
