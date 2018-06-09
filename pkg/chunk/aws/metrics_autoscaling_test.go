package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	promV1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/weaveworks/cortex/pkg/chunk"
)

func TestTableManagerMetricsAutoScaling(t *testing.T) {
	dynamoDB := newMockDynamoDB(0, 0)
	mockProm := mockPrometheus{}

	client := dynamoTableClient{
		DynamoDB: dynamoDB,
		metrics: &metricsData{
			promAPI:           &mockProm,
			queueLengthTarget: 100000,
			tableLastUpdated:  make(map[string]time.Time),
		},
	}

	// Set up table-manager config
	cfg := chunk.SchemaConfig{
		OriginalTableName:   "a",
		UsePeriodicTables:   true,
		IndexTables:         fixturePeriodicTableConfig(tablePrefix, 2, fixtureWriteScale(), fixtureWriteScale()),
		ChunkTables:         fixturePeriodicTableConfig(chunkTablePrefix, 2, fixtureWriteScale(), fixtureWriteScale()),
		CreationGracePeriod: gracePeriod,
	}

	tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
	if err != nil {
		t.Fatal(err)
	}

	// Create tables
	startTime := time.Unix(0, 0).Add(maxChunkAge).Add(gracePeriod)

	test(t, client, tableManager, "Create tables",
		startTime,
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, write, read, write)...),
	)

	mockProm.SetResponse(0, 100000, 100000, []int{0, 0})
	test(t, client, tableManager, "Queues but no errors",
		startTime.Add(time.Minute*10),
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, write, read, write)...), // - remain flat
	)

	mockProm.SetResponse(0, 120000, 100000, []int{100, 200})
	test(t, client, tableManager, "Shrinking queues",
		startTime.Add(time.Minute*20),
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, write, read, write)...), //  - remain flat
	)

	mockProm.SetResponse(0, 120000, 200000, []int{100, 0})
	test(t, client, tableManager, "Building queues",
		startTime.Add(time.Minute*30),
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, 240, read, write)...), // - scale up index table
	)

	mockProm.SetResponse(0, 5000000, 5000000, []int{1, 0})
	test(t, client, tableManager, "Large queues small errors",
		startTime.Add(time.Minute*40),
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, 250, read, write)...), // - scale up index table
	)

	mockProm.SetResponse(0, 0, 0, []int{0, 0})
	test(t, client, tableManager, "No queues no errors",
		startTime.Add(time.Minute*100),
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, 225, read, 180)...), // - scale down both tables
	)

	mockProm.SetResponse(0, 0, 0, []int{0, 0})
	test(t, client, tableManager, "in cooldown period",
		startTime.Add(time.Minute*101),
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, 225, read, 180)...), // - no change; in cooldown period
	)

	mockProm.SetResponse(0, 0, 0, []int{0, 0})
	test(t, client, tableManager, "No queues no errors",
		startTime.Add(time.Minute*200),
		append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, read, 202, read, 162)...), // - scale down both again
	)

	mockProm.SetResponse(0, 0, 0, []int{30, 30, 30, 30})
	test(t, client, tableManager, "Next week",
		startTime.Add(tablePeriod),
		// Nothing much happening - expect table 0 write rates to stay as-is and table 1 to be created with defaults
		append(append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, inactiveRead, 202, inactiveRead, 162)...),
			staticTable(1, read, write, read, write)...),
	)

	// No errors on last week's index table, still some on chunk table
	mockProm.SetResponse(0, 0, 0, []int{0, 30, 30, 30})
	test(t, client, tableManager, "Next week plus a bit",
		startTime.Add(tablePeriod).Add(time.Minute*10),
		append(append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, inactiveRead, 181, inactiveRead, 162)...), // Scale back last week's index table
			staticTable(1, read, write, read, write)...),
	)

	// No errors on last week's tables but some queueing
	mockProm.SetResponse(20000, 20000, 20000, []int{0, 0, 1, 1})
	test(t, client, tableManager, "Next week plus a bit",
		startTime.Add(tablePeriod).Add(time.Minute*20),
		append(append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, inactiveRead, 181, inactiveRead, 162)...), // no scaling back
			staticTable(1, read, write, read, write)...),
	)

	mockProm.SetResponse(120000, 130000, 140000, []int{0, 0, 1, 0})
	test(t, client, tableManager, "next week, queues building, errors on index table",
		startTime.Add(tablePeriod).Add(time.Minute*30),
		append(append(baseTable("a", inactiveRead, inactiveWrite),
			staticTable(0, inactiveRead, 181, inactiveRead, 162)...), // no scaling back
			staticTable(1, read, 240, read, write)...), // scale up index table
	)
}

// Helper to return pre-canned results to Prometheus queries
type mockPrometheus struct {
	rangeValues []model.Value
}

func (m *mockPrometheus) SetResponse(q0, q1, q2 model.SampleValue, errorRates ...[]int) {
	// Mock metrics from Prometheus
	m.rangeValues = []model.Value{
		// Queue lengths
		model.Matrix{
			&model.SampleStream{Values: []model.SamplePair{
				{Timestamp: 0, Value: q0},
				{Timestamp: 15000, Value: q1},
				{Timestamp: 30000, Value: q2},
			}},
		},
	}
	for _, rates := range errorRates {
		errorMatrix := model.Matrix{}
		for i := 0; i < len(rates)/2; i++ {
			errorMatrix = append(errorMatrix,
				&model.SampleStream{
					Metric: model.Metric{"table": model.LabelValue(fmt.Sprintf("cortex_%d", i))},
					Values: []model.SamplePair{{Timestamp: 30000, Value: model.SampleValue(rates[i*2])}},
				},
				&model.SampleStream{
					Metric: model.Metric{"table": model.LabelValue(fmt.Sprintf("chunks_%d", i))},
					Values: []model.SamplePair{{Timestamp: 30000, Value: model.SampleValue(rates[i*2+1])}},
				})
		}
		m.rangeValues = append(m.rangeValues, errorMatrix)
	}
}

func (m *mockPrometheus) Query(ctx context.Context, query string, ts time.Time) (model.Value, error) {
	return nil, errors.New("not implemented")
}

func (m *mockPrometheus) QueryRange(ctx context.Context, query string, r promV1.Range) (model.Value, error) {
	if len(m.rangeValues) == 0 {
		return nil, errors.New("mockPrometheus.QueryRange: out of values")
	}
	// Take the first value and move the slice up
	ret := m.rangeValues[0]
	m.rangeValues = m.rangeValues[1:]
	return ret, nil
}

func (m *mockPrometheus) LabelValues(ctx context.Context, label string) (model.LabelValues, error) {
	return nil, errors.New("not implemented")
}

func (m *mockPrometheus) Series(ctx context.Context, matches []string, startTime time.Time, endTime time.Time) ([]model.LabelSet, error) {
	return nil, errors.New("not implemented")
}

func (m *mockPrometheus) AlertManagers(ctx context.Context) (promV1.AlertManagersResult, error) {
	return promV1.AlertManagersResult{}, errors.New("not implemented")
}

func (m *mockPrometheus) CleanTombstones(ctx context.Context) error {
	return errors.New("not implemented")
}

func (m *mockPrometheus) Config(ctx context.Context) (promV1.ConfigResult, error) {
	return promV1.ConfigResult{}, errors.New("not implemented")
}

func (m *mockPrometheus) DeleteSeries(ctx context.Context, matches []string, startTime time.Time, endTime time.Time) error {
	return errors.New("not implemented")
}

func (m *mockPrometheus) Flags(ctx context.Context) (promV1.FlagsResult, error) {
	return nil, errors.New("not implemented")
}

func (m *mockPrometheus) Snapshot(ctx context.Context, skipHead bool) (promV1.SnapshotResult, error) {
	return promV1.SnapshotResult{}, errors.New("not implemented")
}

func (m *mockPrometheus) Targets(ctx context.Context) (promV1.TargetsResult, error) {
	return promV1.TargetsResult{}, errors.New("not implemented")
}
