package push

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
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
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)
			tsList, err := convertToPromTS(ctx, d, test.cfg, overrides, "user-1", logger)
			require.NoError(t, err)

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
	overrides, err := validation.NewOverrides(querier.DefaultLimitsConfig(), nil)
	require.NoError(b, err)

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
			overrides, err := validation.NewOverrides(querier.DefaultLimitsConfig(), nil)
			require.NoError(t, err)
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
