package util

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

func TestMax(t *testing.T) {
	require.Equal(t, float64(0), max(nil, counterValue))
	require.Equal(t, float64(0), max(&dto.MetricFamily{Metric: nil}, counterValue))
	require.Equal(t, float64(0), max(&dto.MetricFamily{Metric: []*dto.Metric{{Counter: &dto.Counter{}}}}, counterValue))
	require.Equal(t, 12345.6789, max(&dto.MetricFamily{Metric: []*dto.Metric{{Counter: &dto.Counter{Value: proto.Float64(12345.6789)}}}}, counterValue))
	require.Equal(t, 7890.12345, max(&dto.MetricFamily{Metric: []*dto.Metric{
		{Counter: &dto.Counter{Value: proto.Float64(1234.56789)}},
		{Counter: &dto.Counter{Value: proto.Float64(7890.12345)}},
	}}, counterValue))
	// using 'counterValue' as function only works on counters
	require.Equal(t, float64(0), max(&dto.MetricFamily{Metric: []*dto.Metric{
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

// TestSendSumOfGaugesPerUserWithLabels tests to ensure multiple metrics for the same user with a matching label are
// summed correctly
func TestSendSumOfGaugesPerUserWithLabels(t *testing.T) {
	user1Metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_metric"}, []string{"label_one", "label_two"})
	user2Metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_metric"}, []string{"label_one", "label_two"})
	user1Metric.WithLabelValues("a", "b").Set(100)
	user1Metric.WithLabelValues("a", "c").Set(80)
	user2Metric.WithLabelValues("a", "b").Set(60)
	user2Metric.WithLabelValues("a", "c").Set(40)

	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	user1Reg.MustRegister(user1Metric)
	user2Reg.MustRegister(user2Metric)

	mf := BuildMetricFamiliesPerUserFromUserRegistries(map[string]*prometheus.Registry{
		"user-1": user1Reg,
		"user-2": user2Reg,
	})

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one"}, nil)
		actual, err := collectMetrics(func(out chan prometheus.Metric) {
			mf.SendSumOfGaugesPerUserWithLabels(out, desc, "test_metric", "label_one")
		})
		require.NoError(t, err)
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(180)}},
			{Label: makeLabels("label_one", "a", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(100)}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_two"}, nil)
		actual, err := collectMetrics(func(out chan prometheus.Metric) {
			mf.SendSumOfGaugesPerUserWithLabels(out, desc, "test_metric", "label_two")
		})
		require.NoError(t, err)
		expected := []*dto.Metric{
			{Label: makeLabels("label_two", "b", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(100)}},
			{Label: makeLabels("label_two", "c", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(80)}},
			{Label: makeLabels("label_two", "b", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(60)}},
			{Label: makeLabels("label_two", "c", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(40)}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one", "label_two"}, nil)
		actual, err := collectMetrics(func(out chan prometheus.Metric) {
			mf.SendSumOfGaugesPerUserWithLabels(out, desc, "test_metric", "label_one", "label_two")
		})
		require.NoError(t, err)
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(100)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(80)}},
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(60)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(40)}},
		}
		require.ElementsMatch(t, expected, actual)
	}
}

func TestSendMaxOfGauges(t *testing.T) {
	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	desc := prometheus.NewDesc("test_metric", "", nil, nil)

	// No matching metric.
	mf := BuildMetricFamiliesPerUserFromUserRegistries(map[string]*prometheus.Registry{
		"user-1": user1Reg,
		"user-2": user2Reg,
	})
	actual, err := collectMetrics(func(out chan prometheus.Metric) {
		mf.SendMaxOfGauges(out, desc, "test_metric")
	})
	expected := []*dto.Metric{
		{Label: nil, Gauge: &dto.Gauge{Value: proto.Float64(0)}},
	}
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)

	// Register a metric for each user.
	user1Metric := promauto.With(user1Reg).NewGauge(prometheus.GaugeOpts{Name: "test_metric"})
	user2Metric := promauto.With(user2Reg).NewGauge(prometheus.GaugeOpts{Name: "test_metric"})
	user1Metric.Set(100)
	user2Metric.Set(80)
	mf = BuildMetricFamiliesPerUserFromUserRegistries(map[string]*prometheus.Registry{
		"user-1": user1Reg,
		"user-2": user2Reg,
	})

	actual, err = collectMetrics(func(out chan prometheus.Metric) {
		mf.SendMaxOfGauges(out, desc, "test_metric")
	})
	expected = []*dto.Metric{
		{Label: nil, Gauge: &dto.Gauge{Value: proto.Float64(100)}},
	}
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}

func TestSendSumOfHistogramsWithLabels(t *testing.T) {
	buckets := []float64{1, 2, 3}
	user1Metric := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_metric", Buckets: buckets}, []string{"label_one", "label_two"})
	user2Metric := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_metric", Buckets: buckets}, []string{"label_one", "label_two"})
	user1Metric.WithLabelValues("a", "b").Observe(1)
	user1Metric.WithLabelValues("a", "c").Observe(2)
	user2Metric.WithLabelValues("a", "b").Observe(3)
	user2Metric.WithLabelValues("a", "c").Observe(4)

	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	user1Reg.MustRegister(user1Metric)
	user2Reg.MustRegister(user2Metric)

	mf := BuildMetricFamiliesPerUserFromUserRegistries(map[string]*prometheus.Registry{
		"user-1": user1Reg,
		"user-2": user2Reg,
	})

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"label_one"}, nil)
		actual, err := collectMetrics(func(out chan prometheus.Metric) {
			mf.SendSumOfHistogramsWithLabels(out, desc, "test_metric", "label_one")
		})
		require.NoError(t, err)
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a"), Histogram: &dto.Histogram{SampleCount: uint64p(4), SampleSum: float64p(10), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(2)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(3)},
			}}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"label_two"}, nil)
		actual, err := collectMetrics(func(out chan prometheus.Metric) {
			mf.SendSumOfHistogramsWithLabels(out, desc, "test_metric", "label_two")
		})
		require.NoError(t, err)
		expected := []*dto.Metric{
			{Label: makeLabels("label_two", "b"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(4), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(2)},
			}}},
			{Label: makeLabels("label_two", "c"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(6), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(0)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(1)},
			}}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"label_one", "label_two"}, nil)
		actual, err := collectMetrics(func(out chan prometheus.Metric) {
			mf.SendSumOfHistogramsWithLabels(out, desc, "test_metric", "label_one", "label_two")
		})
		require.NoError(t, err)
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "label_two", "b"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(4), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(2)},
			}}},
			{Label: makeLabels("label_one", "a", "label_two", "c"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(6), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(0)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(1)},
			}}},
		}
		require.ElementsMatch(t, expected, actual)
	}
}

func collectMetrics(send func(out chan prometheus.Metric)) ([]*dto.Metric, error) {
	out := make(chan prometheus.Metric)

	go func() {
		send(out)
		close(out)
	}()

	metrics := []*dto.Metric{}
	for m := range out {
		collected := &dto.Metric{}
		err := m.Write(collected)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, collected)
	}

	return metrics, nil
}

func float64p(v float64) *float64 {
	return &v
}

func uint64p(v uint64) *uint64 {
	return &v
}
