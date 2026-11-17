package push

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testMeta holds the metadata the OTLP translator passes through AppendV2Options.
type testMeta struct {
	Type             model.MetricType
	Help             string
	Unit             string
	MetricFamilyName string
}

func (m testMeta) opts(es []exemplar.Exemplar) storage.AppendV2Options {
	return storage.AppendV2Options{
		MetricFamilyName: m.MetricFamilyName,
		Metadata:         metadata.Metadata{Type: m.Type, Help: m.Help, Unit: m.Unit},
		Exemplars:        es,
	}
}

func appendSample(c *collectingAppender, ls labels.Labels, m testMeta, st, t int64, v float64, es []exemplar.Exemplar) error {
	_, err := c.Append(0, ls, st, t, v, nil, nil, m.opts(es))
	return err
}

func appendHistogram(c *collectingAppender, ls labels.Labels, m testMeta, st, t int64, h *histogram.Histogram, es []exemplar.Exemplar) error {
	_, err := c.Append(0, ls, st, t, 0, h, nil, m.opts(es))
	return err
}

func TestCollectingAppender_AppendSample_SingleSeries(t *testing.T) {
	c := newCollectingAppender()
	ls := labels.FromStrings("__name__", "cpu_usage", "job", "test")

	meta := testMeta{
		Type: model.MetricTypeGauge, Help: "CPU usage", Unit: "percent",
		MetricFamilyName: "cpu_usage",
	}

	err := appendSample(c, ls, meta, 0, 1000, 42.5, nil)
	require.NoError(t, err)

	ts := c.TimeSeries()
	require.Len(t, ts, 1)
	assert.Equal(t, []prompb.Label{{Name: "__name__", Value: "cpu_usage"}, {Name: "job", Value: "test"}}, ts[0].Labels)
	require.Len(t, ts[0].Samples, 1)
	assert.Equal(t, 42.5, ts[0].Samples[0].Value)
	assert.Equal(t, int64(1000), ts[0].Samples[0].Timestamp)
	assert.Nil(t, ts[0].Exemplars)
	assert.Nil(t, ts[0].Histograms)

	md := c.Metadata()
	require.Len(t, md, 1)
	assert.Equal(t, prompb.MetricMetadata_GAUGE, md[0].Type)
	assert.Equal(t, "cpu_usage", md[0].MetricFamilyName)
	assert.Equal(t, "CPU usage", md[0].Help)
	assert.Equal(t, "percent", md[0].Unit)
}

func TestCollectingAppender_AppendSample_MultipleSamplesSameSeries(t *testing.T) {
	c := newCollectingAppender()
	ls := labels.FromStrings("__name__", "requests_total")

	meta := testMeta{
		Type:             model.MetricTypeCounter,
		MetricFamilyName: "requests_total",
	}

	require.NoError(t, appendSample(c, ls, meta, 0, 1000, 1, nil))
	require.NoError(t, appendSample(c, ls, meta, 0, 2000, 2, nil))
	require.NoError(t, appendSample(c, ls, meta, 0, 3000, 3, nil))

	ts := c.TimeSeries()
	require.Len(t, ts, 1)
	require.Len(t, ts[0].Samples, 3)
	assert.Equal(t, []prompb.Sample{
		{Value: 1, Timestamp: 1000},
		{Value: 2, Timestamp: 2000},
		{Value: 3, Timestamp: 3000},
	}, ts[0].Samples)
}

func TestCollectingAppender_AppendSample_MultipleSeries(t *testing.T) {
	c := newCollectingAppender()
	meta := testMeta{
		Type:             model.MetricTypeGauge,
		MetricFamilyName: "metric",
	}

	require.NoError(t, appendSample(c, labels.FromStrings("__name__", "metric", "a", "1"), meta, 0, 1000, 10, nil))
	require.NoError(t, appendSample(c, labels.FromStrings("__name__", "metric", "a", "2"), meta, 0, 1000, 20, nil))

	ts := c.TimeSeries()
	require.Len(t, ts, 2)
	// Order is map iteration order; just check we have both label sets and values.
	names := make(map[string]float64)
	for _, s := range ts {
		var a string
		for _, l := range s.Labels {
			if l.Name == "a" {
				a = l.Value
				break
			}
		}
		require.Len(t, s.Samples, 1)
		names[a] = s.Samples[0].Value
	}
	assert.Equal(t, 10.0, names["1"])
	assert.Equal(t, 20.0, names["2"])
}

func TestCollectingAppender_AppendSample_WithExemplars(t *testing.T) {
	c := newCollectingAppender()
	ls := labels.FromStrings("__name__", "latency")
	meta := testMeta{
		Type:             model.MetricTypeHistogram,
		MetricFamilyName: "latency",
	}
	exemplars := []exemplar.Exemplar{
		{Labels: labels.FromStrings("trace_id", "abc"), Value: 0.5, Ts: 1001, HasTs: true},
	}

	err := appendSample(c, ls, meta, 0, 1000, 0.42, exemplars)
	require.NoError(t, err)

	ts := c.TimeSeries()
	require.Len(t, ts, 1)
	require.Len(t, ts[0].Exemplars, 1)
	assert.Equal(t, []prompb.Label{{Name: "trace_id", Value: "abc"}}, ts[0].Exemplars[0].Labels)
	assert.Equal(t, 0.5, ts[0].Exemplars[0].Value)
	assert.Equal(t, int64(1001), ts[0].Exemplars[0].Timestamp)
}

func TestCollectingAppender_AppendHistogram(t *testing.T) {
	c := newCollectingAppender()
	ls := labels.FromStrings("__name__", "request_duration_seconds")
	meta := testMeta{
		Type: model.MetricTypeHistogram, Help: "Request latency", Unit: "seconds",
		MetricFamilyName: "request_duration_seconds",
	}

	h := &histogram.Histogram{
		CounterResetHint: histogram.UnknownCounterReset,
		Schema:           0,
		ZeroThreshold:    0.001,
		ZeroCount:        0,
		Count:            10,
		Sum:              1.5,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets:  []int64{5, 3},
		NegativeSpans:    nil,
		NegativeBuckets:  nil,
	}

	err := appendHistogram(c, ls, meta, 0, 2000, h, nil)
	require.NoError(t, err)

	ts := c.TimeSeries()
	require.Len(t, ts, 1)
	require.Len(t, ts[0].Histograms, 1)
	assert.Equal(t, int64(2000), ts[0].Histograms[0].Timestamp)
	assert.Equal(t, 1.5, ts[0].Histograms[0].Sum)
	assert.Equal(t, int32(0), ts[0].Histograms[0].Schema)
	assert.Nil(t, ts[0].Samples)

	md := c.Metadata()
	require.Len(t, md, 1)
	assert.Equal(t, prompb.MetricMetadata_HISTOGRAM, md[0].Type)
	assert.Equal(t, "request_duration_seconds", md[0].MetricFamilyName)
}

func TestCollectingAppender_Metadata_EmptyMetricFamilyNameIgnored(t *testing.T) {
	c := newCollectingAppender()
	ls := labels.FromStrings("__name__", "y")
	meta := testMeta{
		Type:             model.MetricTypeGauge,
		MetricFamilyName: "",
	}

	err := appendSample(c, ls, meta, 0, 1000, 1, nil)
	require.NoError(t, err)

	md := c.Metadata()
	assert.Len(t, md, 0)
}

func TestCollectingAppender_Metadata_DeduplicatedByFamilyName(t *testing.T) {
	c := newCollectingAppender()
	meta := testMeta{
		Type: model.MetricTypeCounter, Help: "help", Unit: "bytes",
		MetricFamilyName: "http_requests_total",
	}

	require.NoError(t, appendSample(c, labels.FromStrings("__name__", "http_requests_total", "method", "GET"), meta, 0, 1000, 1, nil))
	require.NoError(t, appendSample(c, labels.FromStrings("__name__", "http_requests_total", "method", "POST"), meta, 0, 1000, 1, nil))

	md := c.Metadata()
	require.Len(t, md, 1)
	assert.Equal(t, "http_requests_total", md[0].MetricFamilyName)
}

func TestCollectingAppender_TimeSeries_EmptyInitially(t *testing.T) {
	c := newCollectingAppender()
	assert.Empty(t, c.TimeSeries())
	assert.Empty(t, c.Metadata())
}

func TestCollectingAppender_MixedSamplesAndHistograms(t *testing.T) {
	c := newCollectingAppender()
	ls := labels.FromStrings("__name__", "mixed")
	meta := testMeta{MetricFamilyName: "mixed", Type: model.MetricTypeGauge}

	require.NoError(t, appendSample(c, ls, meta, 0, 1000, 1.0, nil))

	h := &histogram.Histogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           -1,
		ZeroThreshold:    0,
		ZeroCount:        0,
		Count:            1,
		Sum:              2.0,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: 1}},
		PositiveBuckets:  []int64{1},
	}
	require.NoError(t, appendHistogram(c, ls, meta, 0, 2000, h, nil))

	ts := c.TimeSeries()
	require.Len(t, ts, 1)
	require.Len(t, ts[0].Samples, 1)
	require.Len(t, ts[0].Histograms, 1)
	assert.Equal(t, 1.0, ts[0].Samples[0].Value)
	assert.Equal(t, 2.0, ts[0].Histograms[0].Sum)
}
