package push

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

var (
	testHistogram = histogram.Histogram{
		Schema:          2,
		ZeroThreshold:   1e-128,
		ZeroCount:       0,
		Count:           3,
		Sum:             20,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
		PositiveBuckets: []int64{1},
		NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
		NegativeBuckets: []int64{2},
	}
)

func makeV2ReqWithSeries(num int) *writev2.Request {
	ts := make([]writev2.TimeSeries, 0, num)
	symbols := []string{"", "__name__", "test_metric1", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i", "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes"}
	for i := 0; i < num; i++ {
		ts = append(ts, writev2.TimeSeries{
			LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			Metadata: writev2.Metadata{
				Type: writev2.Metadata_METRIC_TYPE_GAUGE,

				HelpRef: 15,
				UnitRef: 16,
			},
			Samples:   []writev2.Sample{{Value: 1, Timestamp: 10}},
			Exemplars: []writev2.Exemplar{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 10}},
			Histograms: []writev2.Histogram{
				writev2.FromIntHistogram(10, &testHistogram),
				writev2.FromFloatHistogram(20, testHistogram.ToFloat(nil)),
			},
		})
	}

	return &writev2.Request{
		Symbols:    symbols,
		Timeseries: ts,
	}
}

func createPRW1HTTPRequest(seriesNum int) (*http.Request, error) {
	series := makeV2ReqWithSeries(seriesNum)
	v1Req, err := convertV2RequestToV1(series)
	if err != nil {
		return nil, err
	}
	protobuf, err := v1Req.Marshal()
	if err != nil {
		return nil, err
	}

	body := snappy.Encode(nil, protobuf)
	req, err := http.NewRequest("POST", "http://localhost/", newResetReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", appProtoContentType)
	req.Header.Set("X-Prometheus-Remote-Write-Version", remoteWriteVersion1HeaderValue)
	req.ContentLength = int64(len(body))
	return req, nil
}

func createPRW2HTTPRequest(seriesNum int) (*http.Request, error) {
	series := makeV2ReqWithSeries(seriesNum)
	protobuf, err := series.Marshal()
	if err != nil {
		return nil, err
	}

	body := snappy.Encode(nil, protobuf)
	req, err := http.NewRequest("POST", "http://localhost/", newResetReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", appProtoV2ContentType)
	req.Header.Set("X-Prometheus-Remote-Write-Version", remoteWriteVersion20HeaderValue)
	req.ContentLength = int64(len(body))
	return req, nil
}

func Benchmark_Handler(b *testing.B) {
	mockHandler := func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
		// Nothing to do.
		return &cortexpb.WriteResponse{}, nil
	}
	testSeriesNums := []int{10, 100, 500, 1000}
	for _, seriesNum := range testSeriesNums {
		b.Run(fmt.Sprintf("PRW1 with %d series", seriesNum), func(b *testing.B) {
			handler := Handler(true, 1000000, nil, mockHandler)
			req, err := createPRW1HTTPRequest(seriesNum)
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				resp := httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				assert.Equal(b, http.StatusOK, resp.Code)
				req.Body.(*resetReader).Reset()
			}
		})
		b.Run(fmt.Sprintf("PRW2 with %d series", seriesNum), func(b *testing.B) {
			handler := Handler(true, 1000000, nil, mockHandler)
			req, err := createPRW2HTTPRequest(seriesNum)
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				resp := httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				assert.Equal(b, http.StatusOK, resp.Code)
				req.Body.(*resetReader).Reset()
			}
		})
	}
}

func Benchmark_convertV2RequestToV1(b *testing.B) {
	testSeriesNums := []int{100, 500, 1000}

	for _, seriesNum := range testSeriesNums {
		b.Run(fmt.Sprintf("%d series", seriesNum), func(b *testing.B) {
			series := makeV2ReqWithSeries(seriesNum)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := convertV2RequestToV1(series)
				require.NoError(b, err)
			}
		})
	}
}

func Test_convertV2RequestToV1(t *testing.T) {
	var v2Req writev2.Request

	fh := tsdbutil.GenerateTestFloatHistogram(1)
	ph := writev2.FromFloatHistogram(4, fh)

	symbols := []string{"", "__name__", "test_metric", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i", "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes"}
	timeseries := []writev2.TimeSeries{
		{
			LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			Metadata: writev2.Metadata{
				Type: writev2.Metadata_METRIC_TYPE_COUNTER,

				HelpRef: 15,
				UnitRef: 16,
			},
			Samples:   []writev2.Sample{{Value: 1, Timestamp: 1}},
			Exemplars: []writev2.Exemplar{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
		},
		{
			LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			Samples:    []writev2.Sample{{Value: 2, Timestamp: 2}},
		},
		{
			LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			Samples:    []writev2.Sample{{Value: 3, Timestamp: 3}},
		},
		{
			LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			Histograms: []writev2.Histogram{ph, ph},
			Exemplars:  []writev2.Exemplar{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
		},
	}

	v2Req.Symbols = symbols
	v2Req.Timeseries = timeseries
	v1Req, err := convertV2RequestToV1(&v2Req)
	assert.NoError(t, err)
	expectedSamples := 3
	expectedExemplars := 2
	expectedHistograms := 2
	countSamples := 0
	countExemplars := 0
	countHistograms := 0

	for _, ts := range v1Req.Timeseries {
		countSamples += len(ts.Samples)
		countExemplars += len(ts.Exemplars)
		countHistograms += len(ts.Histograms)
	}

	assert.Equal(t, expectedSamples, countSamples)
	assert.Equal(t, expectedExemplars, countExemplars)
	assert.Equal(t, expectedHistograms, countHistograms)
	assert.Equal(t, 4, len(v1Req.Timeseries))
	assert.Equal(t, 1, len(v1Req.Metadata))
}

func TestHandler_remoteWrite(t *testing.T) {
	t.Run("remote write v1", func(t *testing.T) {
		handler := Handler(true, 100000, nil, verifyWriteRequestHandler(t, cortexpb.API))
		req := createRequest(t, createPrometheusRemoteWriteProtobuf(t), false)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)
	})
	t.Run("remote write v2", func(t *testing.T) {
		handler := Handler(true, 100000, nil, verifyWriteRequestHandler(t, cortexpb.API))
		req := createRequest(t, createPrometheusRemoteWriteV2Protobuf(t), true)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)

		// test header value
		respHeader := resp.Header()
		assert.Equal(t, "1", respHeader[rw20WrittenSamplesHeader][0])
		assert.Equal(t, "1", respHeader[rw20WrittenHistogramsHeader][0])
		assert.Equal(t, "1", respHeader[rw20WrittenExemplarsHeader][0])
	})
}

func TestHandler_ContentTypeAndEncoding(t *testing.T) {
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(true, 100000, sourceIPs, verifyWriteRequestHandler(t, cortexpb.API))

	tests := []struct {
		description  string
		reqHeaders   map[string]string
		expectedCode int
		isV2         bool
	}{
		{
			description: "[RW 2.0] correct content-type",
			reqHeaders: map[string]string{
				"Content-Type":           appProtoV2ContentType,
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode: http.StatusOK,
			isV2:         true,
		},
		{
			description: "[RW 1.0] correct content-type",
			reqHeaders: map[string]string{
				"Content-Type":           appProtoV1ContentType,
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "0.1.0",
			},
			expectedCode: http.StatusOK,
			isV2:         false,
		},
		{
			description: "[RW 2.0] wrong content-type",
			reqHeaders: map[string]string{
				"Content-Type":           "yolo",
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode: http.StatusUnsupportedMediaType,
			isV2:         true,
		},
		{
			description: "[RW 2.0] wrong content-type",
			reqHeaders: map[string]string{
				"Content-Type":           "application/x-protobuf;proto=yolo",
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode: http.StatusUnsupportedMediaType,
			isV2:         true,
		},
		{
			description: "[RW 2.0] wrong content-encoding",
			reqHeaders: map[string]string{
				"Content-Type":           "application/x-protobuf;proto=io.prometheus.write.v2.Request",
				"Content-Encoding":       "zstd",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode: http.StatusUnsupportedMediaType,
			isV2:         true,
		},
		{
			description:  "no header, should treated as RW 1.0",
			expectedCode: http.StatusOK,
			isV2:         false,
		},
		{
			description: "missing content-type, should treated as RW 1.0",
			reqHeaders: map[string]string{
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode: http.StatusOK,
			isV2:         false,
		},
		{
			description: "missing content-encoding",
			reqHeaders: map[string]string{
				"Content-Type":           appProtoV2ContentType,
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode: http.StatusOK,
			isV2:         true,
		},
		{
			description: "missing remote write version, should treated based on Content-type",
			reqHeaders: map[string]string{
				"Content-Type":     appProtoV2ContentType,
				"Content-Encoding": "snappy",
			},
			expectedCode: http.StatusOK,
			isV2:         true,
		},
		{
			description: "missing remote write version, should treated based on Content-type",
			reqHeaders: map[string]string{
				"Content-Type":     appProtoV1ContentType,
				"Content-Encoding": "snappy",
			},
			expectedCode: http.StatusOK,
			isV2:         false,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.isV2 {
				req := createRequestWithHeaders(t, test.reqHeaders, createCortexRemoteWriteV2Protobuf(t, false, cortexpb.API))
				resp := httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				assert.Equal(t, test.expectedCode, resp.Code)
			} else {
				req := createRequestWithHeaders(t, test.reqHeaders, createCortexWriteRequestProtobuf(t, false, cortexpb.API))
				resp := httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				assert.Equal(t, test.expectedCode, resp.Code)
			}
		})
	}
}

func TestHandler_cortexWriteRequest(t *testing.T) {
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(true, 100000, sourceIPs, verifyWriteRequestHandler(t, cortexpb.API))

	t.Run("remote write v1", func(t *testing.T) {
		req := createRequest(t, createCortexWriteRequestProtobuf(t, false, cortexpb.API), false)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
	})
	t.Run("remote write v2", func(t *testing.T) {
		req := createRequest(t, createCortexRemoteWriteV2Protobuf(t, false, cortexpb.API), true)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
	})
}

func TestHandler_ignoresSkipLabelNameValidationIfSet(t *testing.T) {
	for _, req := range []*http.Request{
		createRequest(t, createCortexWriteRequestProtobuf(t, true, cortexpb.RULE), false),
		createRequest(t, createCortexWriteRequestProtobuf(t, true, cortexpb.RULE), false),
	} {
		resp := httptest.NewRecorder()
		handler := Handler(true, 100000, nil, verifyWriteRequestHandler(t, cortexpb.RULE))
		handler.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
	}
}

func verifyWriteRequestHandler(t *testing.T, expectSource cortexpb.WriteRequest_SourceEnum) func(ctx context.Context, request *cortexpb.WriteRequest) (response *cortexpb.WriteResponse, err error) {
	t.Helper()
	return func(ctx context.Context, request *cortexpb.WriteRequest) (response *cortexpb.WriteResponse, err error) {
		assert.Len(t, request.Timeseries, 1)
		assert.Equal(t, "__name__", request.Timeseries[0].Labels[0].Name)
		assert.Equal(t, "foo", request.Timeseries[0].Labels[0].Value)
		assert.Equal(t, expectSource, request.Source)
		assert.False(t, request.SkipLabelNameValidation)

		resp := &cortexpb.WriteResponse{
			Samples:    1,
			Histograms: 1,
			Exemplars:  1,
		}

		return resp, nil
	}
}

func createRequestWithHeaders(t *testing.T, headers map[string]string, protobuf []byte) *http.Request {
	t.Helper()
	inoutBytes := snappy.Encode(nil, protobuf)
	req, err := http.NewRequest("POST", "http://localhost/", bytes.NewReader(inoutBytes))
	require.NoError(t, err)

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

func createRequest(t *testing.T, protobuf []byte, isV2 bool) *http.Request {
	t.Helper()
	inoutBytes := snappy.Encode(nil, protobuf)
	req, err := http.NewRequest("POST", "http://localhost/", bytes.NewReader(inoutBytes))
	require.NoError(t, err)

	req.Header.Add("Content-Encoding", "snappy")

	if isV2 {
		req.Header.Set("Content-Type", appProtoV2ContentType)
		req.Header.Set("X-Prometheus-Remote-Write-Version", remoteWriteVersion20HeaderValue)
		return req
	}

	req.Header.Set("Content-Type", appProtoContentType)
	req.Header.Set("X-Prometheus-Remote-Write-Version", remoteWriteVersion1HeaderValue)
	return req
}

func createCortexRemoteWriteV2Protobuf(t *testing.T, skipLabelNameValidation bool, source cortexpb.WriteRequest_SourceEnum) []byte {
	t.Helper()
	input := writev2.Request{
		Symbols: []string{"", "__name__", "foo"},
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: []uint32{1, 2},
				Samples: []writev2.Sample{
					{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
			},
		},
	}

	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}

func createPrometheusRemoteWriteV2Protobuf(t *testing.T) []byte {
	t.Helper()
	input := writev2.Request{
		Symbols: []string{"", "__name__", "foo"},
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: []uint32{1, 2},
				Samples: []writev2.Sample{
					{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
			},
		},
	}

	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}

func createPrometheusRemoteWriteProtobuf(t *testing.T) []byte {
	t.Helper()
	input := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "foo"},
				},
				Samples: []prompb.Sample{
					{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
			},
		},
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}
func createCortexWriteRequestProtobuf(t *testing.T, skipLabelNameValidation bool, source cortexpb.WriteRequest_SourceEnum) []byte {
	t.Helper()
	ts := cortexpb.PreallocTimeseries{
		TimeSeries: &cortexpb.TimeSeries{
			Labels: []cortexpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
			},
			Samples: []cortexpb.Sample{
				{Value: 1, TimestampMs: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
			},
		},
	}
	input := cortexpb.WriteRequest{
		Timeseries:              []cortexpb.PreallocTimeseries{ts},
		Source:                  source,
		SkipLabelNameValidation: skipLabelNameValidation,
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}
