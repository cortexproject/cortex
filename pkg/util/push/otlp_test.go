package push

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestOTLP_EnableTypeAndUnitLabels(t *testing.T) {
	logger := log.NewNopLogger()
	ctx := context.Background()
	ts := time.Now()

	tests := []struct {
		description             string
		enableTypeAndUnitLabels bool
		allowDeltaTemporality   bool
		otlpSeries              pmetric.Metric
		expectedLabels          labels.Labels
		expectedMetadata        prompb.MetricMetadata
	}{
		{
			description:             "[enableTypeAndUnitLabels: true], the '__type__' label should be attached when the type is the gauge",
			enableTypeAndUnitLabels: true,
			otlpSeries:              createOtelSum("test", "seconds", pmetric.AggregationTemporalityCumulative, ts),
			expectedLabels: labels.FromMap(map[string]string{
				"__name__":   "test_seconds",
				"__type__":   "gauge",
				"__unit__":   "seconds",
				"test_label": "test_value",
			}),
			expectedMetadata: createPromMetadata("test_seconds", "seconds", prompb.MetricMetadata_GAUGE),
		},
		{
			description:             "[enableTypeAndUnitLabels: true], the '__type__' label should not be attached when the type is unknown",
			enableTypeAndUnitLabels: true,
			allowDeltaTemporality:   true,
			otlpSeries:              createOtelSum("test", "seconds", pmetric.AggregationTemporalityDelta, ts),
			expectedLabels: labels.FromMap(map[string]string{
				"__name__":   "test_seconds",
				"__unit__":   "seconds",
				"test_label": "test_value",
			}),
			expectedMetadata: createPromMetadata("test_seconds", "seconds", prompb.MetricMetadata_UNKNOWN),
		},
		{
			description:             "[enableTypeAndUnitLabels: false]",
			enableTypeAndUnitLabels: false,
			otlpSeries:              createOtelSum("test", "seconds", pmetric.AggregationTemporalityCumulative, ts),
			expectedLabels: labels.FromMap(map[string]string{
				"__name__":   "test_seconds",
				"test_label": "test_value",
			}),
			expectedMetadata: createPromMetadata("test_seconds", "seconds", prompb.MetricMetadata_GAUGE),
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			cfg := distributor.OTLPConfig{
				EnableTypeAndUnitLabels: test.enableTypeAndUnitLabels,
				AllowDeltaTemporality:   test.allowDeltaTemporality,
			}
			metrics := pmetric.NewMetrics()
			rm := metrics.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()

			test.otlpSeries.CopyTo(sm.Metrics().AppendEmpty())

			limits := validation.Limits{}
			overrides := validation.NewOverrides(limits, nil)
			promSeries, metadata, err := convertToPromTS(ctx, metrics, cfg, overrides, "user-1", logger)
			require.NoError(t, err)
			require.Equal(t, 1, len(promSeries))
			require.Equal(t, prompb.FromLabels(test.expectedLabels, nil), promSeries[0].Labels)

			require.Equal(t, 1, len(metadata))
			require.Equal(t, test.expectedMetadata, metadata[0])
		})
	}
}

func TestOTLP_AllowDeltaTemporality(t *testing.T) {
	logger := log.NewNopLogger()
	ctx := context.Background()
	ts := time.Now()

	tests := []struct {
		description           string
		allowDeltaTemporality bool
		otlpSeries            []pmetric.Metric
		expectedSeries        []prompb.TimeSeries
		expectedMetadata      []prompb.MetricMetadata
		expectedErr           string
	}{
		{
			description:           "[allowDeltaTemporality: false] cumulative type should be converted",
			allowDeltaTemporality: false,
			otlpSeries: []pmetric.Metric{
				createOtelSum("test_1", "", pmetric.AggregationTemporalityCumulative, ts),
				createOtelSum("test_2", "", pmetric.AggregationTemporalityCumulative, ts),
			},
			expectedSeries: []prompb.TimeSeries{
				createPromFloatSeries("test_1", ts),
				createPromFloatSeries("test_2", ts),
			},
			expectedMetadata: []prompb.MetricMetadata{
				createPromMetadata("test_1", "", prompb.MetricMetadata_GAUGE),
				createPromMetadata("test_2", "", prompb.MetricMetadata_GAUGE),
			},
		},
		{
			description:           "[allowDeltaTemporality: false] delta type should not be converted",
			allowDeltaTemporality: false,
			otlpSeries: []pmetric.Metric{
				createOtelSum("test_1", "", pmetric.AggregationTemporalityDelta, ts),
				createOtelSum("test_2", "", pmetric.AggregationTemporalityDelta, ts),
			},
			expectedSeries:   []prompb.TimeSeries{},
			expectedMetadata: []prompb.MetricMetadata{},
			expectedErr:      `invalid temporality and type combination for metric "test_1"; invalid temporality and type combination for metric "test_2"`,
		},
		{
			description:           "[allowDeltaTemporality: true] delta type should be converted",
			allowDeltaTemporality: true,
			otlpSeries: []pmetric.Metric{
				createOtelSum("test_1", "", pmetric.AggregationTemporalityDelta, ts),
				createOtelSum("test_2", "", pmetric.AggregationTemporalityDelta, ts),
			},
			expectedSeries: []prompb.TimeSeries{
				createPromFloatSeries("test_1", ts),
				createPromFloatSeries("test_2", ts),
			},
			expectedMetadata: []prompb.MetricMetadata{
				createPromMetadata("test_1", "", prompb.MetricMetadata_UNKNOWN),
				createPromMetadata("test_2", "", prompb.MetricMetadata_UNKNOWN),
			},
		},
		{
			description:           "[allowDeltaTemporality: false] mixed delta and cumulative, should be converted only for cumulative type",
			allowDeltaTemporality: false,
			otlpSeries: []pmetric.Metric{
				createOtelSum("test_1", "", pmetric.AggregationTemporalityDelta, ts),
				createOtelSum("test_2", "", pmetric.AggregationTemporalityCumulative, ts),
			},
			expectedSeries: []prompb.TimeSeries{
				createPromFloatSeries("test_2", ts),
			},
			expectedMetadata: []prompb.MetricMetadata{
				createPromMetadata("test_2", "", prompb.MetricMetadata_GAUGE),
			},
			expectedErr: `invalid temporality and type combination for metric "test_1"`,
		},
		{
			description:           "[allowDeltaTemporality: false, exponential histogram] cumulative histogram should be converted",
			allowDeltaTemporality: false,
			otlpSeries: []pmetric.Metric{
				createOtelExponentialHistogram("test_1", pmetric.AggregationTemporalityCumulative, ts),
				createOtelExponentialHistogram("test_2", pmetric.AggregationTemporalityCumulative, ts),
			},
			expectedSeries: []prompb.TimeSeries{
				createPromNativeHistogramSeries("test_1", prompb.Histogram_UNKNOWN, ts),
				createPromNativeHistogramSeries("test_2", prompb.Histogram_UNKNOWN, ts),
			},
			expectedMetadata: []prompb.MetricMetadata{
				createPromMetadata("test_1", "", prompb.MetricMetadata_HISTOGRAM),
				createPromMetadata("test_2", "", prompb.MetricMetadata_HISTOGRAM),
			},
		},
		{
			description:           "[allowDeltaTemporality: false, exponential histogram] delta histogram should not be converted",
			allowDeltaTemporality: false,
			otlpSeries: []pmetric.Metric{
				createOtelExponentialHistogram("test_1", pmetric.AggregationTemporalityDelta, ts),
				createOtelExponentialHistogram("test_2", pmetric.AggregationTemporalityDelta, ts),
			},
			expectedSeries:   []prompb.TimeSeries{},
			expectedMetadata: []prompb.MetricMetadata{},
			expectedErr:      `invalid temporality and type combination for metric "test_1"; invalid temporality and type combination for metric "test_2"`,
		},
		{
			description:           "[allowDeltaTemporality: true, exponential histogram] delta histogram should be converted",
			allowDeltaTemporality: true,
			otlpSeries: []pmetric.Metric{
				createOtelExponentialHistogram("test_1", pmetric.AggregationTemporalityDelta, ts),
				createOtelExponentialHistogram("test_2", pmetric.AggregationTemporalityDelta, ts),
			},
			expectedSeries: []prompb.TimeSeries{
				createPromNativeHistogramSeries("test_1", prompb.Histogram_GAUGE, ts),
				createPromNativeHistogramSeries("test_2", prompb.Histogram_GAUGE, ts),
			},
			expectedMetadata: []prompb.MetricMetadata{
				createPromMetadata("test_1", "", prompb.MetricMetadata_UNKNOWN),
				createPromMetadata("test_2", "", prompb.MetricMetadata_UNKNOWN),
			},
		},
		{
			description:           "[allowDeltaTemporality: false, exponential histogram] mixed delta and cumulative histogram, should be converted only for cumulative type",
			allowDeltaTemporality: false,
			otlpSeries: []pmetric.Metric{
				createOtelExponentialHistogram("test_1", pmetric.AggregationTemporalityDelta, ts),
				createOtelExponentialHistogram("test_2", pmetric.AggregationTemporalityCumulative, ts),
			},
			expectedSeries: []prompb.TimeSeries{
				createPromNativeHistogramSeries("test_2", prompb.Histogram_UNKNOWN, ts),
			},
			expectedMetadata: []prompb.MetricMetadata{
				createPromMetadata("test_2", "", prompb.MetricMetadata_HISTOGRAM),
			},
			expectedErr: `invalid temporality and type combination for metric "test_1"`,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			cfg := distributor.OTLPConfig{AllowDeltaTemporality: test.allowDeltaTemporality}
			metrics := pmetric.NewMetrics()
			rm := metrics.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()

			for _, s := range test.otlpSeries {
				s.CopyTo(sm.Metrics().AppendEmpty())
			}

			limits := validation.Limits{}
			overrides := validation.NewOverrides(limits, nil)
			promSeries, metadata, err := convertToPromTS(ctx, metrics, cfg, overrides, "user-1", logger)
			require.Equal(t, sortTimeSeries(test.expectedSeries), sortTimeSeries(promSeries))
			require.Equal(t, test.expectedMetadata, metadata)
			if test.expectedErr != "" {
				require.Equal(t, test.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
			}

		})
	}
}

func createPromMetadata(name, unit string, metadataType prompb.MetricMetadata_MetricType) prompb.MetricMetadata {
	return prompb.MetricMetadata{
		Type:             metadataType,
		Unit:             unit,
		MetricFamilyName: name,
	}
}

// copied from: https://github.com/prometheus/prometheus/blob/v3.5.0/storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw.go
func sortTimeSeries(series []prompb.TimeSeries) []prompb.TimeSeries {
	for i := range series {
		sort.Slice(series[i].Labels, func(j, k int) bool {
			return series[i].Labels[j].Name < series[i].Labels[k].Name
		})
	}

	sort.Slice(series, func(i, j int) bool {
		return fmt.Sprint(series[i].Labels) < fmt.Sprint(series[j].Labels)
	})

	return series
}

// copied from: https://github.com/prometheus/prometheus/blob/v3.5.0/storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw.go
func createPromFloatSeries(name string, ts time.Time) prompb.TimeSeries {
	return prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: name},
			{Name: "test_label", Value: "test_value"},
		},
		Samples: []prompb.Sample{{
			Value:     5,
			Timestamp: ts.UnixMilli(),
		}},
	}
}

// copied from: https://github.com/prometheus/prometheus/blob/v3.5.0/storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw.go
func createOtelSum(name, unit string, temporality pmetric.AggregationTemporality, ts time.Time) pmetric.Metric {
	metrics := pmetric.NewMetricSlice()
	m := metrics.AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(temporality)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(5)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.Attributes().PutStr("test_label", "test_value")
	return m
}

// copied from: https://github.com/prometheus/prometheus/blob/v3.5.0/storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw.go
func createOtelExponentialHistogram(name string, temporality pmetric.AggregationTemporality, ts time.Time) pmetric.Metric {
	metrics := pmetric.NewMetricSlice()
	m := metrics.AppendEmpty()
	m.SetName(name)
	hist := m.SetEmptyExponentialHistogram()
	hist.SetAggregationTemporality(temporality)
	dp := hist.DataPoints().AppendEmpty()
	dp.SetCount(1)
	dp.SetSum(5)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.Attributes().PutStr("test_label", "test_value")
	return m
}

// copied from: https://github.com/prometheus/prometheus/blob/v3.5.0/storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw.go
func createPromNativeHistogramSeries(name string, hint prompb.Histogram_ResetHint, ts time.Time) prompb.TimeSeries {
	return prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: name},
			{Name: "test_label", Value: "test_value"},
		},
		Histograms: []prompb.Histogram{
			{
				Count:         &prompb.Histogram_CountInt{CountInt: 1},
				Sum:           5,
				Schema:        0,
				ZeroThreshold: 1e-128,
				ZeroCount:     &prompb.Histogram_ZeroCountInt{ZeroCountInt: 0},
				Timestamp:     ts.UnixMilli(),
				ResetHint:     hint,
			},
		},
	}
}

func TestOTLPConvertToPromTS(t *testing.T) {
	logger := log.NewNopLogger()
	ctx := context.Background()
	d := pmetric.NewMetrics()
	resourceMetric := d.ResourceMetrics().AppendEmpty()
	resourceMetric.Resource().Attributes().PutStr("service.name", "test-service") // converted to job, service_name
	resourceMetric.Resource().Attributes().PutStr("attr1", "value")
	resourceMetric.Resource().Attributes().PutStr("attr2", "value")
	resourceMetric.Resource().Attributes().PutStr("attr3", "value")

	scopeMetric := resourceMetric.ScopeMetrics().AppendEmpty()

	//Generate One Counter
	timestamp := time.Now()
	counterMetric := scopeMetric.Metrics().AppendEmpty()
	counterMetric.SetName("test-counter")
	counterMetric.SetDescription("test-counter-description")
	counterMetric.SetEmptySum()
	counterMetric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	counterMetric.Sum().SetIsMonotonic(true)

	counterDataPoint := counterMetric.Sum().DataPoints().AppendEmpty()
	counterDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	counterDataPoint.SetDoubleValue(10.0)

	tests := []struct {
		description               string
		PromoteResourceAttributes []string
		cfg                       distributor.OTLPConfig
		expectedLabels            []prompb.Label
	}{
		{
			description:               "target_info should be generated and an attribute that exist in promote resource attributes should be converted",
			PromoteResourceAttributes: []string{"attr1"},
			cfg: distributor.OTLPConfig{
				ConvertAllAttributes: false,
				DisableTargetInfo:    false,
			},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_counter_total",
				},
				{
					Name:  "attr1",
					Value: "value",
				},
				{
					Name:  "job",
					Value: "test-service",
				},
			},
		},
		{
			description:               "an attributes that exist in promote resource attributes should be converted",
			PromoteResourceAttributes: []string{"attr1"},
			cfg: distributor.OTLPConfig{
				ConvertAllAttributes: false,
				DisableTargetInfo:    true,
			},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_counter_total",
				},
				{
					Name:  "attr1",
					Value: "value",
				},
				{
					Name:  "job",
					Value: "test-service",
				},
			},
		},
		{
			description:               "not exist attribute is ignored",
			PromoteResourceAttributes: []string{"dummy"},
			cfg: distributor.OTLPConfig{
				ConvertAllAttributes: false,
				DisableTargetInfo:    true,
			},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_counter_total",
				},
				{
					Name:  "job",
					Value: "test-service",
				},
			},
		},
		{
			description:               "should convert all attribute",
			PromoteResourceAttributes: nil,
			cfg: distributor.OTLPConfig{
				ConvertAllAttributes: true,
				DisableTargetInfo:    true,
			},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_counter_total",
				},
				{
					Name:  "attr1",
					Value: "value",
				},
				{
					Name:  "attr2",
					Value: "value",
				},
				{
					Name:  "attr3",
					Value: "value",
				},
				{
					Name:  "job",
					Value: "test-service",
				},
				{
					Name:  "service_name",
					Value: "test-service",
				},
			},
		},
		{
			description:               "should convert all attribute regardless of promote resource attributes",
			PromoteResourceAttributes: []string{"attr1", "attr2"},
			cfg: distributor.OTLPConfig{
				ConvertAllAttributes: true,
				DisableTargetInfo:    true,
			},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_counter_total",
				},
				{
					Name:  "attr1",
					Value: "value",
				},
				{
					Name:  "attr2",
					Value: "value",
				},
				{
					Name:  "attr3",
					Value: "value",
				},
				{
					Name:  "job",
					Value: "test-service",
				},
				{
					Name:  "service_name",
					Value: "test-service",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			limits := validation.Limits{
				PromoteResourceAttributes: test.PromoteResourceAttributes,
			}
			overrides := validation.NewOverrides(limits, nil)
			tsList, metadata, err := convertToPromTS(ctx, d, test.cfg, overrides, "user-1", logger)
			require.NoError(t, err)

			// test metadata conversion
			require.Equal(t, 1, len(metadata))
			require.Equal(t, prompb.MetricMetadata_MetricType(1), metadata[0].Type)
			require.Equal(t, "test_counter_total", metadata[0].MetricFamilyName)
			require.Equal(t, "test-counter-description", metadata[0].Help)

			if test.cfg.DisableTargetInfo {
				require.Equal(t, 1, len(tsList)) // test_counter_total
			} else {
				// target_info should exist
				require.Equal(t, 2, len(tsList)) // test_counter_total + target_info
			}

			var counterTs prompb.TimeSeries
			for _, ts := range tsList {
				for _, label := range ts.Labels {
					if label.Name == "__name__" && label.Value == "test_counter_total" {
						// get counter ts
						counterTs = ts
					}
				}
			}

			require.ElementsMatch(t, test.expectedLabels, counterTs.Labels)
		})
	}
}

// for testing
type resetReader struct {
	*bytes.Reader
	body []byte
}

func newResetReader(body []byte) *resetReader {
	return &resetReader{
		Reader: bytes.NewReader(body),
		body:   body,
	}
}

func (r *resetReader) Reset() {
	r.Reader.Reset(r.body)
}

func (r *resetReader) Close() error {
	return nil
}

func getOTLPHttpRequest(otlpRequest *pmetricotlp.ExportRequest, contentType, encodingType string) (*http.Request, error) {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "user-1")

	var body []byte
	var err error
	switch contentType {
	case jsonContentType:
		body, err = otlpRequest.MarshalJSON()
		if err != nil {
			return nil, err
		}
	case pbContentType:
		body, err = otlpRequest.MarshalProto()
		if err != nil {
			return nil, err
		}
	}

	if encodingType == "gzip" {
		var gzipBody bytes.Buffer
		gz := gzip.NewWriter(&gzipBody)
		_, err = gz.Write(body)
		if err != nil {
			return nil, err
		}
		if err = gz.Close(); err != nil {
			return nil, err
		}
		body = gzipBody.Bytes()
	}

	req, err := http.NewRequestWithContext(ctx, "", "", newResetReader(body))
	if err != nil {
		return nil, err
	}

	switch contentType {
	case jsonContentType:
		req.Header.Set("Content-Type", jsonContentType)
	case pbContentType:
		req.Header.Set("Content-Type", pbContentType)
	}

	if encodingType != "" {
		req.Header.Set("Content-Encoding", encodingType)
	}
	req.ContentLength = int64(len(body))

	return req, nil
}

func BenchmarkOTLPWriteHandler(b *testing.B) {
	cfg := distributor.OTLPConfig{
		ConvertAllAttributes: false,
		DisableTargetInfo:    false,
	}
	overrides := validation.NewOverrides(querier.DefaultLimitsConfig(), nil)

	exportRequest := generateOTLPWriteRequest()
	mockPushFunc := func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
		return &cortexpb.WriteResponse{}, nil
	}
	handler := OTLPHandler(10000, overrides, cfg, nil, mockPushFunc)

	b.Run("json with no compression", func(b *testing.B) {
		req, err := getOTLPHttpRequest(&exportRequest, jsonContentType, "")
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			resp := recorder.Result()
			require.Equal(b, http.StatusOK, resp.StatusCode)
			req.Body.(*resetReader).Reset()
		}
	})
	b.Run("json with gzip", func(b *testing.B) {
		req, err := getOTLPHttpRequest(&exportRequest, jsonContentType, "gzip")
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			resp := recorder.Result()
			require.Equal(b, http.StatusOK, resp.StatusCode)
			req.Body.(*resetReader).Reset()
		}
	})
	b.Run("proto with no compression", func(b *testing.B) {
		req, err := getOTLPHttpRequest(&exportRequest, pbContentType, "")
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			resp := recorder.Result()
			require.Equal(b, http.StatusOK, resp.StatusCode)
			req.Body.(*resetReader).Reset()
		}
	})
	b.Run("proto with gzip", func(b *testing.B) {
		req, err := getOTLPHttpRequest(&exportRequest, pbContentType, "gzip")
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			resp := recorder.Result()
			require.Equal(b, http.StatusOK, resp.StatusCode)
			req.Body.(*resetReader).Reset()
		}
	})
}

func TestOTLPWriteHandler(t *testing.T) {
	cfg := distributor.OTLPConfig{
		ConvertAllAttributes: false,
		DisableTargetInfo:    false,
	}

	exportRequest := generateOTLPWriteRequest()

	tests := []struct {
		description        string
		maxRecvMsgSize     int
		contentType        string
		expectedStatusCode int
		expectedErrMsg     string
		encodingType       string
	}{
		{
			description:        "Test proto format write with no compression",
			maxRecvMsgSize:     10000,
			contentType:        pbContentType,
			expectedStatusCode: http.StatusOK,
		},
		{
			description:        "Test proto format write with gzip",
			maxRecvMsgSize:     10000,
			contentType:        pbContentType,
			expectedStatusCode: http.StatusOK,
			encodingType:       "gzip",
		},
		{
			description:        "Test json format write with no compression",
			maxRecvMsgSize:     10000,
			contentType:        jsonContentType,
			expectedStatusCode: http.StatusOK,
		},
		{
			description:        "Test json format write with gzip",
			maxRecvMsgSize:     10000,
			contentType:        jsonContentType,
			expectedStatusCode: http.StatusOK,
			encodingType:       "gzip",
		},
		{
			description:        "request too big than maxRecvMsgSize (proto) with no compression",
			maxRecvMsgSize:     10,
			contentType:        pbContentType,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrMsg:     "received message larger than max",
		},
		{
			description:        "request too big than maxRecvMsgSize (proto) with gzip",
			maxRecvMsgSize:     10,
			contentType:        pbContentType,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrMsg:     "received message larger than max",
			encodingType:       "gzip",
		},
		{
			description:        "request too big than maxRecvMsgSize (json) with no compression",
			maxRecvMsgSize:     10,
			contentType:        jsonContentType,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrMsg:     "received message larger than max",
		},
		{
			description:        "request too big than maxRecvMsgSize (json) with gzip",
			maxRecvMsgSize:     10,
			contentType:        jsonContentType,
			expectedStatusCode: http.StatusBadRequest,
			expectedErrMsg:     "received message larger than max",
			encodingType:       "gzip",
		},
		{
			description:        "invalid encoding type: snappy",
			maxRecvMsgSize:     10000,
			contentType:        jsonContentType,
			expectedStatusCode: http.StatusBadRequest,
			encodingType:       "snappy",
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			req, err := getOTLPHttpRequest(&exportRequest, test.contentType, test.encodingType)
			require.NoError(t, err)

			push := verifyOTLPWriteRequestHandler(t, cortexpb.API)
			overrides := validation.NewOverrides(querier.DefaultLimitsConfig(), nil)
			handler := OTLPHandler(test.maxRecvMsgSize, overrides, cfg, nil, push)

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			resp := recorder.Result()
			require.Equal(t, test.expectedStatusCode, resp.StatusCode)

			if test.expectedErrMsg != "" {
				b, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Contains(t, string(b), test.expectedErrMsg)
			}
		})
	}
}

func generateOTLPWriteRequest() pmetricotlp.ExportRequest {
	d := pmetric.NewMetrics()

	// Generate One Counter, One Gauge, One Histogram, One Exponential-Histogram
	// with resource attributes: service.name="test-service", service.instance.id="test-instance", host.name="test-host"
	// with metric attibute: foo.bar="baz"

	timestamp := time.Now()

	resourceMetric := d.ResourceMetrics().AppendEmpty()
	resourceMetric.Resource().Attributes().PutStr("service.name", "test-service")
	resourceMetric.Resource().Attributes().PutStr("service.instance.id", "test-instance")
	resourceMetric.Resource().Attributes().PutStr("host.name", "test-host")

	scopeMetric := resourceMetric.ScopeMetrics().AppendEmpty()

	// Generate One Counter
	counterMetric := scopeMetric.Metrics().AppendEmpty()
	counterMetric.SetName("test-counter")
	counterMetric.SetDescription("test-counter-description")
	counterMetric.SetEmptySum()
	counterMetric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	counterMetric.Sum().SetIsMonotonic(true)

	counterDataPoint := counterMetric.Sum().DataPoints().AppendEmpty()
	counterDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	counterDataPoint.SetDoubleValue(10.0)
	counterDataPoint.Attributes().PutStr("foo.bar", "baz")

	counterExemplar := counterDataPoint.Exemplars().AppendEmpty()
	counterExemplar.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	counterExemplar.SetDoubleValue(10.0)
	counterExemplar.SetSpanID(pcommon.SpanID{0, 1, 2, 3, 4, 5, 6, 7})
	counterExemplar.SetTraceID(pcommon.TraceID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})

	// Generate One Gauge
	gaugeMetric := scopeMetric.Metrics().AppendEmpty()
	gaugeMetric.SetName("test-gauge")
	gaugeMetric.SetDescription("test-gauge-description")
	gaugeMetric.SetEmptyGauge()

	gaugeDataPoint := gaugeMetric.Gauge().DataPoints().AppendEmpty()
	gaugeDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	gaugeDataPoint.SetDoubleValue(10.0)
	gaugeDataPoint.Attributes().PutStr("foo.bar", "baz")

	// Generate One Histogram
	histogramMetric := scopeMetric.Metrics().AppendEmpty()
	histogramMetric.SetName("test-histogram")
	histogramMetric.SetDescription("test-histogram-description")
	histogramMetric.SetEmptyHistogram()
	histogramMetric.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	histogramDataPoint := histogramMetric.Histogram().DataPoints().AppendEmpty()
	histogramDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	histogramDataPoint.ExplicitBounds().FromRaw([]float64{0.0, 1.0, 2.0, 3.0, 4.0, 5.0})
	histogramDataPoint.BucketCounts().FromRaw([]uint64{2, 2, 2, 2, 2, 2})
	histogramDataPoint.SetCount(10)
	histogramDataPoint.SetSum(30.0)
	histogramDataPoint.Attributes().PutStr("foo.bar", "baz")

	// Generate One Exponential-Histogram
	exponentialHistogramMetric := scopeMetric.Metrics().AppendEmpty()
	exponentialHistogramMetric.SetName("test-exponential-histogram")
	exponentialHistogramMetric.SetDescription("test-exponential-histogram-description")
	exponentialHistogramMetric.SetEmptyExponentialHistogram()
	exponentialHistogramMetric.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	exponentialHistogramDataPoint := exponentialHistogramMetric.ExponentialHistogram().DataPoints().AppendEmpty()
	exponentialHistogramDataPoint.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	exponentialHistogramDataPoint.SetScale(2.0)
	exponentialHistogramDataPoint.Positive().BucketCounts().FromRaw([]uint64{2, 2, 2, 2, 2})
	exponentialHistogramDataPoint.SetZeroCount(2)
	exponentialHistogramDataPoint.SetCount(10)
	exponentialHistogramDataPoint.SetSum(30.0)
	exponentialHistogramDataPoint.Attributes().PutStr("foo.bar", "baz")

	return pmetricotlp.NewExportRequestFromMetrics(d)
}

func verifyOTLPWriteRequestHandler(t *testing.T, expectSource cortexpb.WriteRequest_SourceEnum) func(ctx context.Context, request *cortexpb.WriteRequest) (response *cortexpb.WriteResponse, err error) {
	t.Helper()
	return func(ctx context.Context, request *cortexpb.WriteRequest) (response *cortexpb.WriteResponse, err error) {
		assert.Len(t, request.Timeseries, 13) // 1 (target_info) + 1 (counter) + 1 (gauge) + 7 (hist_bucket) + 2 (hist_sum, hist_count) + 1 (exponential histogram)
		// TODO: test more things
		assert.Equal(t, expectSource, request.Source)
		assert.False(t, request.SkipLabelNameValidation)
		for _, ts := range request.Timeseries {
			assert.NotEmpty(t, ts.Labels)
			// Make sure at least one of sample, exemplar or histogram is set.
			assert.True(t, len(ts.Samples) > 0 || len(ts.Exemplars) > 0 || len(ts.Histograms) > 0)
		}
		return &cortexpb.WriteResponse{}, nil
	}
}
