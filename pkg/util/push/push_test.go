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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
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

func makeV2ReqWithSeries(num int) *cortexpb.PreallocWriteRequestV2 {
	ts := make([]cortexpb.PreallocTimeseriesV2, 0, num)
	symbols := []string{"", "__name__", "test_metric1", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i", "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes"}
	for range num {
		ts = append(ts, cortexpb.PreallocTimeseriesV2{
			TimeSeriesV2: &cortexpb.TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				Metadata: cortexpb.MetadataV2{
					Type: cortexpb.METRIC_TYPE_GAUGE,

					HelpRef: 15,
					UnitRef: 16,
				},
				Samples:   []cortexpb.Sample{{Value: 1, TimestampMs: 10}},
				Exemplars: []cortexpb.ExemplarV2{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 10}},
				Histograms: []cortexpb.Histogram{
					cortexpb.HistogramToHistogramProto(10, &testHistogram),
					cortexpb.FloatHistogramToHistogramProto(20, testHistogram.ToFloat(nil)),
				},
			},
		})
	}

	return &cortexpb.PreallocWriteRequestV2{
		WriteRequestV2: cortexpb.WriteRequestV2{
			Symbols:    symbols,
			Timeseries: ts,
		},
	}
}

func createPRW1HTTPRequest(seriesNum int) (*http.Request, error) {
	series := makeV2ReqWithSeries(seriesNum)
	v1Req, err := convertV2RequestToV1(series, false)
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
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	mockHandler := func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
		// Nothing to do.
		return &cortexpb.WriteResponse{}, nil
	}
	testSeriesNums := []int{10, 100, 500, 1000}
	for _, seriesNum := range testSeriesNums {
		b.Run(fmt.Sprintf("PRW1 with %d series", seriesNum), func(b *testing.B) {
			handler := Handler(true, false, 1000000, overrides, nil, mockHandler, nil)
			req, err := createPRW1HTTPRequest(seriesNum)
			require.NoError(b, err)

			b.ReportAllocs()

			for b.Loop() {
				resp := httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				assert.Equal(b, http.StatusOK, resp.Code)
				req.Body.(*resetReader).Reset()
			}
		})
		b.Run(fmt.Sprintf("PRW2 with %d series", seriesNum), func(b *testing.B) {
			handler := Handler(true, false, 1000000, overrides, nil, mockHandler, nil)
			req, err := createPRW2HTTPRequest(seriesNum)
			require.NoError(b, err)

			b.ReportAllocs()

			for b.Loop() {
				resp := httptest.NewRecorder()
				handler.ServeHTTP(resp, req)
				assert.Equal(b, http.StatusNoContent, resp.Code)
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

			b.ReportAllocs()
			for b.Loop() {
				_, err := convertV2RequestToV1(series, false)
				require.NoError(b, err)
			}
		})
	}
}

func Test_convertV2RequestToV1_WithEnableTypeAndUnitLabels(t *testing.T) {
	symbols := []string{"", "__name__", "test_metric1", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i", "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes", "__type__", "exist type", "__unit__", "exist unit"}
	samples := []cortexpb.Sample{
		{
			Value:       123,
			TimestampMs: 123,
		},
	}

	tests := []struct {
		desc                    string
		v2Req                   *cortexpb.PreallocWriteRequestV2
		expectedV1Req           cortexpb.PreallocWriteRequest
		enableTypeAndUnitLabels bool
	}{
		{
			desc: "should attach unit and type labels when the enableTypeAndUnitLabels is true",
			v2Req: &cortexpb.PreallocWriteRequestV2{
				WriteRequestV2: cortexpb.WriteRequestV2{
					Symbols: symbols,
					Timeseries: []cortexpb.PreallocTimeseriesV2{
						{
							TimeSeriesV2: &cortexpb.TimeSeriesV2{
								LabelsRefs: []uint32{1, 2, 3, 4},
								Samples:    samples,
								Metadata:   cortexpb.MetadataV2{Type: cortexpb.METRIC_TYPE_COUNTER, HelpRef: 15, UnitRef: 16},
								Exemplars:  []cortexpb.ExemplarV2{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
							},
						},
					},
				},
			},
			expectedV1Req: cortexpb.PreallocWriteRequest{
				WriteRequest: cortexpb.WriteRequest{
					Timeseries: []cortexpb.PreallocTimeseries{
						{
							TimeSeries: &cortexpb.TimeSeries{
								Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "test_metric1", "__type__", "counter", "__unit__", "Maybe op/sec who knows (:", "b", "c")),
								Samples: samples,
								Exemplars: []cortexpb.Exemplar{
									{
										Labels:      cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("f", "g")),
										Value:       1,
										TimestampMs: 1,
									},
								},
							},
						},
					},
					Metadata: []*cortexpb.MetricMetadata{
						{
							Type:             cortexpb.COUNTER,
							MetricFamilyName: "test_metric1",
							Help:             "Test gauge for test purposes",
							Unit:             "Maybe op/sec who knows (:",
						},
					},
				},
			},
			enableTypeAndUnitLabels: true,
		},
		{
			desc: "should be added from metadata when __type__ and __unit__ labels already exist.",
			v2Req: &cortexpb.PreallocWriteRequestV2{
				WriteRequestV2: cortexpb.WriteRequestV2{
					Symbols: symbols,
					Timeseries: []cortexpb.PreallocTimeseriesV2{
						{
							TimeSeriesV2: &cortexpb.TimeSeriesV2{
								LabelsRefs: []uint32{1, 2, 3, 4, 18, 19, 20, 21},
								Samples:    samples,
								Metadata:   cortexpb.MetadataV2{Type: cortexpb.METRIC_TYPE_COUNTER, HelpRef: 15, UnitRef: 16},
								Exemplars:  []cortexpb.ExemplarV2{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
							},
						},
					},
				},
			},
			expectedV1Req: cortexpb.PreallocWriteRequest{
				WriteRequest: cortexpb.WriteRequest{
					Timeseries: []cortexpb.PreallocTimeseries{
						{
							TimeSeries: &cortexpb.TimeSeries{
								Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "test_metric1", "__type__", "counter", "__unit__", "Maybe op/sec who knows (:", "b", "c")),
								Samples: samples,
								Exemplars: []cortexpb.Exemplar{
									{
										Labels:      cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("f", "g")),
										Value:       1,
										TimestampMs: 1,
									},
								},
							},
						},
					},
					Metadata: []*cortexpb.MetricMetadata{
						{
							Type:             cortexpb.COUNTER,
							MetricFamilyName: "test_metric1",
							Help:             "Test gauge for test purposes",
							Unit:             "Maybe op/sec who knows (:",
						},
					},
				},
			},
			enableTypeAndUnitLabels: true,
		},
		{
			desc: "should not attach unit and type labels when the enableTypeAndUnitLabels is false",
			v2Req: &cortexpb.PreallocWriteRequestV2{
				WriteRequestV2: cortexpb.WriteRequestV2{
					Symbols: symbols,
					Timeseries: []cortexpb.PreallocTimeseriesV2{
						{
							TimeSeriesV2: &cortexpb.TimeSeriesV2{
								LabelsRefs: []uint32{1, 2, 3, 4},
								Samples:    samples,
								Metadata:   cortexpb.MetadataV2{Type: cortexpb.METRIC_TYPE_COUNTER, HelpRef: 15, UnitRef: 16},
								Exemplars:  []cortexpb.ExemplarV2{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
							},
						},
					},
				},
			},
			expectedV1Req: cortexpb.PreallocWriteRequest{
				WriteRequest: cortexpb.WriteRequest{
					Timeseries: []cortexpb.PreallocTimeseries{
						{
							TimeSeries: &cortexpb.TimeSeries{
								Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "test_metric1", "b", "c")),
								Samples: samples,
								Exemplars: []cortexpb.Exemplar{
									{
										Labels:      cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("f", "g")),
										Value:       1,
										TimestampMs: 1,
									},
								},
							},
						},
					},
					Metadata: []*cortexpb.MetricMetadata{
						{
							Type:             cortexpb.COUNTER,
							MetricFamilyName: "test_metric1",
							Help:             "Test gauge for test purposes",
							Unit:             "Maybe op/sec who knows (:",
						},
					},
				},
			},
			enableTypeAndUnitLabels: false,
		},
		{
			desc: "should not attach when type is unknown and unit is empty although the enableTypeAndUnitLabels is true",
			v2Req: &cortexpb.PreallocWriteRequestV2{
				WriteRequestV2: cortexpb.WriteRequestV2{
					Symbols: symbols,
					Timeseries: []cortexpb.PreallocTimeseriesV2{
						{
							TimeSeriesV2: &cortexpb.TimeSeriesV2{
								LabelsRefs: []uint32{1, 2, 3, 4},
								Samples:    samples,
								Metadata:   cortexpb.MetadataV2{Type: cortexpb.METRIC_TYPE_UNSPECIFIED, HelpRef: 15, UnitRef: 0},
								Exemplars:  []cortexpb.ExemplarV2{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
							},
						},
					},
				},
			},
			expectedV1Req: cortexpb.PreallocWriteRequest{
				WriteRequest: cortexpb.WriteRequest{
					Timeseries: []cortexpb.PreallocTimeseries{
						{
							TimeSeries: &cortexpb.TimeSeries{
								Labels:  cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "test_metric1", "b", "c")),
								Samples: samples,
								Exemplars: []cortexpb.Exemplar{
									{
										Labels:      cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("f", "g")),
										Value:       1,
										TimestampMs: 1,
									},
								},
							},
						},
					},
					Metadata: []*cortexpb.MetricMetadata{
						{
							Type:             cortexpb.UNKNOWN,
							MetricFamilyName: "test_metric1",
							Help:             "Test gauge for test purposes",
						},
					},
				},
			},
			enableTypeAndUnitLabels: false,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			v1Req, err := convertV2RequestToV1(test.v2Req, test.enableTypeAndUnitLabels)
			require.NoError(t, err)
			require.Equal(t, test.expectedV1Req, v1Req)
		})
	}
}

func Test_convertV2RequestToV1(t *testing.T) {
	var v2Req cortexpb.PreallocWriteRequestV2

	fh := tsdbutil.GenerateTestFloatHistogram(1)
	ph := cortexpb.FloatHistogramToHistogramProto(4, fh)

	symbols := []string{"", "__name__", "test_metric", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i", "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes"}
	timeseries := []cortexpb.PreallocTimeseriesV2{
		{
			TimeSeriesV2: &cortexpb.TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				Metadata: cortexpb.MetadataV2{
					Type: cortexpb.METRIC_TYPE_COUNTER,

					HelpRef: 15,
					UnitRef: 16,
				},
				Samples:   []cortexpb.Sample{{Value: 1, TimestampMs: 1}},
				Exemplars: []cortexpb.ExemplarV2{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
			},
		},
		{
			TimeSeriesV2: &cortexpb.TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				Samples:    []cortexpb.Sample{{Value: 2, TimestampMs: 2}},
			},
		},
		{
			TimeSeriesV2: &cortexpb.TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				Samples:    []cortexpb.Sample{{Value: 3, TimestampMs: 3}},
			},
		},
		{
			TimeSeriesV2: &cortexpb.TimeSeriesV2{
				LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				Histograms: []cortexpb.Histogram{ph, ph},
				Exemplars:  []cortexpb.ExemplarV2{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: 1}},
			},
		},
	}

	v2Req.Symbols = symbols
	v2Req.Timeseries = timeseries
	v1Req, err := convertV2RequestToV1(&v2Req, false)
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
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	tests := []struct {
		name           string
		createBody     func() ([]byte, bool) // returns (bodyBytes, isV2)
		expectedStatus int
		expectedBody   string
		verifyResponse func(resp *httptest.ResponseRecorder)
	}{
		{
			name: "remote write v1",
			createBody: func() ([]byte, bool) {
				return createPrometheusRemoteWriteProtobuf(t), false
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "remote write v2",
			createBody: func() ([]byte, bool) {
				return createPrometheusRemoteWriteV2Protobuf(t), true
			},
			expectedStatus: http.StatusNoContent,
			verifyResponse: func(resp *httptest.ResponseRecorder) {
				respHeader := resp.Header()
				assert.Equal(t, "1", respHeader[rw20WrittenSamplesHeader][0])
				assert.Equal(t, "1", respHeader[rw20WrittenHistogramsHeader][0])
				assert.Equal(t, "1", respHeader[rw20WrittenExemplarsHeader][0])
			},
		},
		{
			name: "remote write v2 with empty samples and histograms should return 400",
			createBody: func() ([]byte, bool) {
				// Create a request with a TimeSeries that has no samples and no histograms
				reqProto := writev2.Request{
					Symbols: []string{"", "__name__", "foo"},
					Timeseries: []writev2.TimeSeries{
						{
							LabelsRefs: []uint32{1, 2},
							Exemplars: []writev2.Exemplar{
								{
									LabelsRefs: []uint32{},
									Value:      1.0,
									Timestamp:  time.Now().UnixMilli(),
								},
							},
						},
					},
				}
				reqBytes, err := reqProto.Marshal()
				require.NoError(t, err)
				return reqBytes, true
			},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "TimeSeries must contain at least one sample or histogram for series {__name__=\"foo\"}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, "user-1")
			handler := Handler(true, false, 100000, overrides, nil, verifyWriteRequestHandler(t, cortexpb.API), nil)

			body, isV2 := test.createBody()
			req := createRequest(t, body, isV2)
			req = req.WithContext(ctx)

			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			assert.Equal(t, test.expectedStatus, resp.Code)

			if test.expectedBody != "" {
				assert.Contains(t, resp.Body.String(), test.expectedBody)
			}

			if test.verifyResponse != nil {
				test.verifyResponse(resp)
			}
		})
	}

	t.Run("remote write v2 HA dedup", func(t *testing.T) {
		// HA dedup: push returns error with StatusAccepted (202) but also a non-nil WriteResponse with stats.
		dedupWriteResp := &cortexpb.WriteResponse{
			Samples:    5,
			Histograms: 2,
			Exemplars:  1,
		}
		dedupErr := httpgrpc.Errorf(http.StatusAccepted, "HA deduplication: samples deduped")
		pushFunc := func(ctx context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
			return dedupWriteResp, dedupErr
		}

		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "user-1")
		handler := Handler(true, false, 100000, overrides, nil, pushFunc, nil)
		req := createRequest(t, createPrometheusRemoteWriteV2Protobuf(t), true)
		req = req.WithContext(ctx)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusAccepted, resp.Code)
		respHeader := resp.Header()
		assert.Equal(t, "5", respHeader[rw20WrittenSamplesHeader][0])
		assert.Equal(t, "2", respHeader[rw20WrittenHistogramsHeader][0])
		assert.Equal(t, "1", respHeader[rw20WrittenExemplarsHeader][0])
		assert.Contains(t, resp.Body.String(), "HA deduplication")
	})
}

func TestHandler_ContentTypeAndEncoding(t *testing.T) {
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")

	tests := []struct {
		description                         string
		reqHeaders                          map[string]string
		expectedCode                        int
		isV2                                bool
		remoteWrite2Enabled                 bool
		acceptUnknownRemoteWriteContentType bool
	}{
		{
			description: "[RW 2.0] correct content-type",
			reqHeaders: map[string]string{
				"Content-Type":           appProtoV2ContentType,
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode:        http.StatusNoContent,
			isV2:                true,
			remoteWrite2Enabled: true,
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
			expectedCode:        http.StatusUnsupportedMediaType,
			isV2:                true,
			remoteWrite2Enabled: true,
		},
		{
			description: "[RW 2.0] wrong content-encoding",
			reqHeaders: map[string]string{
				"Content-Type":           "application/x-protobuf;proto=io.prometheus.write.v2.Request",
				"Content-Encoding":       "zstd",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode:        http.StatusUnsupportedMediaType,
			isV2:                true,
			remoteWrite2Enabled: true,
		},
		{
			description: "[RW 2.0] V2 disabled",
			reqHeaders: map[string]string{
				"Content-Type":           appProtoV2ContentType,
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode:        http.StatusUnsupportedMediaType,
			isV2:                true,
			remoteWrite2Enabled: false,
		},
		{
			description:         "no header, should treated as RW 1.0",
			expectedCode:        http.StatusOK,
			isV2:                false,
			remoteWrite2Enabled: true,
		},
		{
			description: "missing content-type, should treated as RW 1.0",
			reqHeaders: map[string]string{
				"Content-Encoding":       "snappy",
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode:        http.StatusOK,
			isV2:                false,
			remoteWrite2Enabled: true,
		},
		{
			description: "missing content-encoding",
			reqHeaders: map[string]string{
				"Content-Type":           appProtoV2ContentType,
				remoteWriteVersionHeader: "2.0.0",
			},
			expectedCode:        http.StatusNoContent,
			isV2:                true,
			remoteWrite2Enabled: true,
		},
		{
			description: "missing remote write version, should treated based on Content-type",
			reqHeaders: map[string]string{
				"Content-Type":     appProtoV2ContentType,
				"Content-Encoding": "snappy",
			},
			expectedCode:        http.StatusNoContent,
			isV2:                true,
			remoteWrite2Enabled: true,
		},
		{
			description: "missing remote write version, should treated based on Content-type",
			reqHeaders: map[string]string{
				"Content-Type":     appProtoV1ContentType,
				"Content-Encoding": "snappy",
			},
			expectedCode:        http.StatusOK,
			isV2:                false,
			remoteWrite2Enabled: true,
		},
		{
			description: "unknown content-type with acceptUnknownRemoteWriteContentType, treated as RW v1",
			reqHeaders: map[string]string{
				"Content-Type":     "yolo",
				"Content-Encoding": "snappy",
			},
			expectedCode:                        http.StatusOK,
			isV2:                                false,
			remoteWrite2Enabled:                 true,
			acceptUnknownRemoteWriteContentType: true,
		},
		{
			description: "invalid proto param with acceptUnknownRemoteWriteContentType, treated as RW v1",
			reqHeaders: map[string]string{
				"Content-Type":     "application/x-protobuf;proto=yolo",
				"Content-Encoding": "snappy",
			},
			expectedCode:                        http.StatusOK,
			isV2:                                false,
			remoteWrite2Enabled:                 true,
			acceptUnknownRemoteWriteContentType: true,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			handler := Handler(test.remoteWrite2Enabled, test.acceptUnknownRemoteWriteContentType, 100000, overrides, sourceIPs, verifyWriteRequestHandler(t, cortexpb.API), nil)

			if test.isV2 {
				ctx := context.Background()
				ctx = user.InjectOrgID(ctx, "user-1")

				req := createRequestWithHeaders(t, test.reqHeaders, createCortexRemoteWriteV2Protobuf(t, false, cortexpb.API))
				req = req.WithContext(ctx)
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
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(true, false, 100000, overrides, sourceIPs, verifyWriteRequestHandler(t, cortexpb.API), nil)

	t.Run("remote write v1", func(t *testing.T) {
		req := createRequest(t, createCortexWriteRequestProtobuf(t, false, cortexpb.API), false)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
	})
	t.Run("remote write v2", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "user-1")

		req := createRequest(t, createCortexRemoteWriteV2Protobuf(t, false, cortexpb.API), true)
		req = req.WithContext(ctx)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNoContent, resp.Code)
	})
}

func TestHandler_ignoresSkipLabelNameValidationIfSet(t *testing.T) {
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	for _, req := range []*http.Request{
		createRequest(t, createCortexWriteRequestProtobuf(t, true, cortexpb.RULE), false),
		createRequest(t, createCortexWriteRequestProtobuf(t, true, cortexpb.RULE), false),
	} {
		resp := httptest.NewRecorder()
		handler := Handler(true, false, 100000, overrides, nil, verifyWriteRequestHandler(t, cortexpb.RULE), nil)
		handler.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
	}
}

func TestHandler_MetricCollection(t *testing.T) {
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_counter",
		Help: "test help",
	}, []string{"type"})

	handler := Handler(true, false, 100000, overrides, nil, verifyWriteRequestHandler(t, cortexpb.API), counter)

	t.Run("counts v1 requests", func(t *testing.T) {
		req := createRequest(t, createPrometheusRemoteWriteProtobuf(t), false)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)

		val := testutil.ToFloat64(counter.WithLabelValues("prw1"))
		assert.Equal(t, 1.0, val)
	})

	t.Run("counts v2 requests", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "user-1")

		req := createRequest(t, createPrometheusRemoteWriteV2Protobuf(t), true)
		req = req.WithContext(ctx)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusNoContent, resp.Code)

		val := testutil.ToFloat64(counter.WithLabelValues("prw2"))
		assert.Equal(t, 1.0, val)
	})

	t.Run("counts unknown or invalid content-type as unknown when acceptUnknownRemoteWriteContentType is true", func(t *testing.T) {
		handlerWithUnknown := Handler(true, true, 100000, overrides, nil, verifyWriteRequestHandler(t, cortexpb.API), counter)
		req := createRequestWithHeaders(t, map[string]string{
			"Content-Type":     "yolo",
			"Content-Encoding": "snappy",
		}, createCortexWriteRequestProtobuf(t, false, cortexpb.API))
		resp := httptest.NewRecorder()
		handlerWithUnknown.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)

		val := testutil.ToFloat64(counter.WithLabelValues("unknown"))
		assert.Equal(t, 1.0, val)
	})
}

func verifyWriteRequestHandler(t *testing.T, expectSource cortexpb.SourceEnum) func(ctx context.Context, request *cortexpb.WriteRequest) (response *cortexpb.WriteResponse, err error) {
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

func createCortexRemoteWriteV2Protobuf(t *testing.T, skipLabelNameValidation bool, source cortexpb.SourceEnum) []byte {
	t.Helper()

	input := cortexpb.WriteRequestV2{
		Symbols: []string{"", "__name__", "foo"},
		Timeseries: []cortexpb.PreallocTimeseriesV2{
			{
				TimeSeriesV2: &cortexpb.TimeSeriesV2{
					LabelsRefs: []uint32{1, 2},
					Samples: []cortexpb.Sample{
						{Value: 1, TimestampMs: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
					},
				},
			},
		},
		Source:                  source,
		SkipLabelNameValidation: skipLabelNameValidation,
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
func createCortexWriteRequestProtobuf(t *testing.T, skipLabelNameValidation bool, source cortexpb.SourceEnum) []byte {
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

// TestHandler_RemoteWriteV2_MetadataPoolReset verifies that reusing a WriteRequestV2
// from the sync.Pool does not carry over omitted Metadata fields (UnitRef and HelpRef),
// which would otherwise cause an index-out-of-range panic
func TestHandler_RemoteWriteV2_MetadataPoolReset(t *testing.T) {
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	mockPush := func(ctx context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
		return &cortexpb.WriteResponse{}, nil
	}

	handler := Handler(true, 1000000, overrides, nil, mockPush, nil)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	sendRequest := func(reqProto *writev2.Request) *httptest.ResponseRecorder {
		reqBytes, err := reqProto.Marshal()
		require.NoError(t, err)

		req := createRequest(t, reqBytes, true).WithContext(ctx)
		resp := httptest.NewRecorder()

		// Should not panic
		require.NotPanics(t, func() {
			handler.ServeHTTP(resp, req)
		})

		return resp
	}

	symbols := make([]string, 100)
	symbols[0], symbols[1], symbols[2] = "", "__name__", "test_metric"
	for i := 3; i < 100; i++ {
		symbols[i] = fmt.Sprintf("sym-%d", i)
	}

	req1Proto := writev2.Request{
		Symbols: symbols,
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: []uint32{1, 2},
				Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
				Metadata: writev2.Metadata{
					Type:    writev2.Metadata_METRIC_TYPE_COUNTER,
					UnitRef: 99,
					HelpRef: 98,
				},
			},
		},
	}

	resp1 := sendRequest(&req1Proto)
	require.Equal(t, http.StatusNoContent, resp1.Code)

	req2Proto := writev2.Request{
		Symbols: []string{"", "__name__", "foo", "bar"},
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: []uint32{1, 2},
				Samples:    []writev2.Sample{{Value: 2, Timestamp: 2000}},
			},
		},
	}

	resp2 := sendRequest(&req2Proto)
	require.Equal(t, http.StatusNoContent, resp2.Code)
}
