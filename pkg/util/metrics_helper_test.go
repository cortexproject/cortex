package util

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestSum(t *testing.T) {
	require.Equal(t, float64(0), sum(nil, counterValue))
	require.Equal(t, float64(0), sum(&dto.MetricFamily{Metric: nil}, counterValue))
	require.Equal(t, float64(0), sum(&dto.MetricFamily{Metric: []*dto.Metric{{Counter: &dto.Counter{}}}}, counterValue))
	require.Equal(t, 12345.6789, sum(&dto.MetricFamily{Metric: []*dto.Metric{{Counter: &dto.Counter{Value: proto.Float64(12345.6789)}}}}, counterValue))
	require.Equal(t, 20235.80235, sum(&dto.MetricFamily{Metric: []*dto.Metric{
		{Counter: &dto.Counter{Value: proto.Float64(12345.6789)}},
		{Counter: &dto.Counter{Value: proto.Float64(7890.12345)}},
	}}, counterValue))
	// using 'counterValue' as function only sums counters
	require.Equal(t, float64(0), sum(&dto.MetricFamily{Metric: []*dto.Metric{
		{Gauge: &dto.Gauge{Value: proto.Float64(12345.6789)}},
		{Gauge: &dto.Gauge{Value: proto.Float64(7890.12345)}},
	}}, counterValue))
}

func TestCounterValue(t *testing.T) {
	require.Equal(t, float64(0), counterValue(nil))
	require.Equal(t, float64(0), counterValue(&dto.Metric{}))
	require.Equal(t, float64(0), counterValue(&dto.Metric{Counter: &dto.Counter{}}))
	require.Equal(t, float64(543857.12837), counterValue(&dto.Metric{Counter: &dto.Counter{Value: proto.Float64(543857.12837)}}))
}

func TestGetMetricsWithLabelNames(t *testing.T) {
	labels := []string{"a", "b"}

	require.Equal(t, map[string]metricsWithLabels{}, getMetricsWithLabelNames(nil, labels))
	require.Equal(t, map[string]metricsWithLabels{}, getMetricsWithLabelNames(&dto.MetricFamily{}, labels))

	m1 := &dto.Metric{Label: makeLabels("a", "5"), Counter: &dto.Counter{Value: proto.Float64(1)}}
	m2 := &dto.Metric{Label: makeLabels("a", "10", "b", "20"), Counter: &dto.Counter{Value: proto.Float64(1.5)}}
	m3 := &dto.Metric{Label: makeLabels("a", "10", "b", "20", "c", "1"), Counter: &dto.Counter{Value: proto.Float64(2)}}
	m4 := &dto.Metric{Label: makeLabels("a", "10", "b", "20", "c", "2"), Counter: &dto.Counter{Value: proto.Float64(3)}}
	m5 := &dto.Metric{Label: makeLabels("a", "11", "b", "21"), Counter: &dto.Counter{Value: proto.Float64(4)}}
	m6 := &dto.Metric{Label: makeLabels("ignored", "123", "a", "12", "b", "22", "c", "30"), Counter: &dto.Counter{Value: proto.Float64(4)}}

	out := getMetricsWithLabelNames(&dto.MetricFamily{Metric: []*dto.Metric{m1, m2, m3, m4, m5, m6}}, labels)

	// m1 is not returned at all, as it doesn't have both required labels.
	require.Equal(t, map[string]metricsWithLabels{
		getLabelsString([]string{"10", "20"}): {
			labelValues: []string{"10", "20"},
			metrics:     []*dto.Metric{m2, m3, m4}},
		getLabelsString([]string{"11", "21"}): {
			labelValues: []string{"11", "21"},
			metrics:     []*dto.Metric{m5}},
		getLabelsString([]string{"12", "22"}): {
			labelValues: []string{"12", "22"},
			metrics:     []*dto.Metric{m6}},
	}, out)

	// no labels -- returns all metrics in single key. this isn't very efficient, and there are other functions
	// (without labels) to handle this better, but it still works.
	out2 := getMetricsWithLabelNames(&dto.MetricFamily{Metric: []*dto.Metric{m1, m2, m3, m4, m5, m6}}, nil)
	require.Equal(t, map[string]metricsWithLabels{
		getLabelsString(nil): {
			labelValues: []string{},
			metrics:     []*dto.Metric{m1, m2, m3, m4, m5, m6}},
	}, out2)
}

func makeLabels(namesAndValues ...string) []*dto.LabelPair {
	out := []*dto.LabelPair(nil)

	for i := 0; i+1 < len(namesAndValues); i = i + 2 {
		out = append(out, &dto.LabelPair{
			Name:  proto.String(namesAndValues[i]),
			Value: proto.String(namesAndValues[i+1]),
		})
	}

	return out
}
