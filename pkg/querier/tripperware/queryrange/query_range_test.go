package queryrange

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestRequest(t *testing.T) {
	t.Parallel()
	// Create a Copy parsedRequest to assign the expected headers to the request without affecting other tests using the global.
	// The test below adds a Test-Header header to the request and expects it back once the encode/decode of request is done via PrometheusCodec
	parsedRequestWithHeaders := *parsedRequest
	parsedRequestWithHeaders.Headers = reqHeaders
	for _, tc := range []struct {
		url         string
		expected    tripperware.Request
		expectedErr error
	}{
		{
			url:      query,
			expected: &parsedRequestWithHeaders,
		},
		{
			url:         "api/v1/query_range?start=foo&stats=all",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "invalid parameter \"start\"; cannot parse \"foo\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=bar",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "invalid parameter \"end\"; cannot parse \"bar\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=0",
			expectedErr: errEndBeforeStart,
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=baz",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "invalid parameter \"step\"; cannot parse \"baz\" to a valid duration"),
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=-1",
			expectedErr: errNegativeStep,
		},
		{
			url:         "api/v1/query_range?start=0&end=11001&step=1",
			expectedErr: errStepTooSmall,
		},
	} {
		tc := tc
		t.Run(tc.url, func(t *testing.T) {
			t.Parallel()
			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)
			r.Header.Add("Test-Header", "test")

			ctx := user.InjectOrgID(context.Background(), "1")

			// Get a deep copy of the request with Context changed to ctx
			r = r.Clone(ctx)

			req, err := PrometheusCodec.DecodeRequest(ctx, r, []string{"Test-Header"})
			if err != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := PrometheusCodec.EncodeRequest(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.url, rdash.RequestURI)
		})
	}
}

func TestResponse(t *testing.T) {
	t.Parallel()
	r := *parsedResponse
	rWithWarnings := *parsedResponseWithWarnings
	r.Headers = respHeaders
	rWithWarnings.Headers = respHeaders
	for i, tc := range []struct {
		body                  string
		expected              *tripperware.PrometheusResponse
		expectedDecodeErr     error
		cancelCtxBeforeDecode bool
	}{
		{
			body:     responseBody,
			expected: &r,
		},
		{
			body:     responseBodyWithWarnings,
			expected: &rWithWarnings,
		},
		{
			body:                  responseBody,
			cancelCtxBeforeDecode: true,
			expectedDecodeErr:     context.Canceled,
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ctx, cancelCtx := context.WithCancel(context.Background())
			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			if tc.cancelCtxBeforeDecode {
				cancelCtx()
			}
			resp, err := PrometheusCodec.DecodeResponse(ctx, response, nil)
			assert.Equal(t, tc.expectedDecodeErr, err)
			if err != nil {
				cancelCtx()
				return
			}
			assert.Equal(t, tc.expected, resp)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
				ContentLength: int64(len(tc.body)),
			}
			resp2, err := PrometheusCodec.EncodeResponse(context.Background(), resp)
			require.NoError(t, err)
			assert.Equal(t, response, resp2)
			cancelCtx()
		})
	}
}

func TestResponseWithStats(t *testing.T) {
	t.Parallel()
	for i, tc := range []struct {
		body     string
		expected *tripperware.PrometheusResponse
	}{
		{
			body: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]],"peakSamples":16}}}}`,
			expected: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "foo", Value: "bar"},
										},
										Samples: []cortexpb.Sample{
											{Value: 137, TimestampMs: 1536673680000},
											{Value: 137, TimestampMs: 1536673780000},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{
						Samples: &tripperware.PrometheusResponseSamplesStats{
							TotalQueryableSamples: 10,
							TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
								{Value: 5, TimestampMs: 1536673680000},
								{Value: 5, TimestampMs: 1536673780000},
							},
							PeakSamples: 16,
						},
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			tc.expected.Headers = respHeaders
			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			resp, err := PrometheusCodec.DecodeResponse(context.Background(), response, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, resp)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
				ContentLength: int64(len(tc.body)),
			}
			resp2, err := PrometheusCodec.EncodeResponse(context.Background(), resp)
			require.NoError(t, err)
			assert.Equal(t, response, resp2)
		})
	}
}

func TestMergeAPIResponses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		input       []tripperware.Response
		expected    tripperware.Response
		expectedErr error
		cancelCtx   bool
	}{
		{
			name:  "No responses shouldn't panic and return a non-null result and result type.",
			input: []tripperware.Response{},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{},
					},
				},
			},
		},
		{
			name: "A single empty response shouldn't panic.",
			input: []tripperware.Response{
				&tripperware.PrometheusResponse{
					Status: StatusSuccess,
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{},
						},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{},
					},
				},
			},
		},
		{
			name: "Multiple empty responses shouldn't panic.",
			input: []tripperware.Response{
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{},
						},
					},
				},
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{},
						},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{},
							},
						},
					},
				},
			},
		},
		{
			name: "Basic merging of two responses.",
			input: []tripperware.Response{
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{},
											Samples: []cortexpb.Sample{
												{Value: 0, TimestampMs: 0},
												{Value: 1, TimestampMs: 1},
											},
										},
									},
								},
							},
						},
					},
				},
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{},
											Samples: []cortexpb.Sample{
												{Value: 2, TimestampMs: 2},
												{Value: 3, TimestampMs: 3},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{},
										Samples: []cortexpb.Sample{
											{Value: 0, TimestampMs: 0},
											{Value: 1, TimestampMs: 1},
											{Value: 2, TimestampMs: 2},
											{Value: 3, TimestampMs: 3},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of responses when labels are in different order.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[0,"0"],[1,"1"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 0, TimestampMs: 0},
											{Value: 1, TimestampMs: 1000},
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is single overlap.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 1, TimestampMs: 1000},
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is multiple partial overlaps.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 1, TimestampMs: 1000},
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
											{Value: 4, TimestampMs: 4000},
											{Value: 5, TimestampMs: 5000},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merge response with warnings.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","warnings":["warning1","warning2"],"data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"]]}]}}`),
				mustParse(t, `{"status":"success","warnings":["warning1","warning3"],"data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"]]}]}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status:   StatusSuccess,
				Warnings: []string{"warning1", "warning2", "warning3"},
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 1, TimestampMs: 1000},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is complete overlap.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
											{Value: 4, TimestampMs: 4000},
											{Value: 5, TimestampMs: 5000},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Context cancel should cancel merge",
			input: []tripperware.Response{
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{},
											Samples: []cortexpb.Sample{
												{Value: 0, TimestampMs: 0},
												{Value: 1, TimestampMs: 1},
											},
										},
									},
								},
							},
						},
					},
				},
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{},
											Samples: []cortexpb.Sample{
												{Value: 2, TimestampMs: 2},
												{Value: 3, TimestampMs: 3},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			cancelCtx:   true,
			expectedErr: context.Canceled,
		},
		{
			name: "[stats] A single empty response shouldn't panic.",
			input: []tripperware.Response{
				&tripperware.PrometheusResponse{
					Status: StatusSuccess,
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{},
						},
						Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{}},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{}},
				},
			},
		},
		{
			name: "[stats] Multiple empty responses shouldn't panic.",
			input: []tripperware.Response{
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{}},
					},
				},
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{}},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{}},
				},
			},
		},
		{
			name: "[stats] Basic merging of two responses.",
			input: []tripperware.Response{
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{},
											Samples: []cortexpb.Sample{
												{Value: 0, TimestampMs: 0},
												{Value: 1, TimestampMs: 1},
											},
										},
									},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
							TotalQueryableSamples: 20,
							TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
								{Value: 5, TimestampMs: 0},
								{Value: 15, TimestampMs: 1},
							},
						}},
					},
				},
				&tripperware.PrometheusResponse{
					Data: tripperware.PrometheusData{
						ResultType: matrix,
						Result: tripperware.PrometheusQueryResult{
							Result: &tripperware.PrometheusQueryResult_Matrix{
								Matrix: &tripperware.Matrix{
									SampleStreams: []tripperware.SampleStream{
										{
											Labels: []cortexpb.LabelAdapter{},
											Samples: []cortexpb.Sample{
												{Value: 2, TimestampMs: 2},
												{Value: 3, TimestampMs: 3},
											},
										},
									},
								},
							},
						},
						Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
							TotalQueryableSamples: 10,
							TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
								{Value: 5, TimestampMs: 2},
								{Value: 5, TimestampMs: 3},
							},
						}},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{},
										Samples: []cortexpb.Sample{
											{Value: 0, TimestampMs: 0},
											{Value: 1, TimestampMs: 1},
											{Value: 2, TimestampMs: 2},
											{Value: 3, TimestampMs: 3},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
						TotalQueryableSamples: 30,
						TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 5, TimestampMs: 0},
							{Value: 15, TimestampMs: 1},
							{Value: 5, TimestampMs: 2},
							{Value: 5, TimestampMs: 3},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of samples where there is single overlap.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,5],[2,5]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,5],[3,15]]}}}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 1, TimestampMs: 1000},
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
						TotalQueryableSamples: 25,
						TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 5, TimestampMs: 1000},
							{Value: 5, TimestampMs: 2000},
							{Value: 15, TimestampMs: 3000},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of multiple responses with some overlap.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[3,"3"],[4,"4"],[5,"5"]]}],"stats":{"samples":{"totalQueryableSamples":12,"totalQueryableSamplesPerStep":[[3,3],[4,4],[5,5]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"],[4,"4"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3],[4,4]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[5,"5"],[6,"6"],[7,"7"]]}],"stats":{"samples":{"totalQueryableSamples":18,"totalQueryableSamplesPerStep":[[5,5],[6,6],[7,7]]}}}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 1, TimestampMs: 1000},
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
											{Value: 4, TimestampMs: 4000},
											{Value: 5, TimestampMs: 5000},
											{Value: 6, TimestampMs: 6000},
											{Value: 7, TimestampMs: 7000},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
						TotalQueryableSamples: 28,
						TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 1, TimestampMs: 1000},
							{Value: 2, TimestampMs: 2000},
							{Value: 3, TimestampMs: 3000},
							{Value: 4, TimestampMs: 4000},
							{Value: 5, TimestampMs: 5000},
							{Value: 6, TimestampMs: 6000},
							{Value: 7, TimestampMs: 7000},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of samples where there is multiple partial overlaps.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3],[4,4],[5,5]]}}}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 1, TimestampMs: 1000},
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
											{Value: 4, TimestampMs: 4000},
											{Value: 5, TimestampMs: 5000},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
						TotalQueryableSamples: 15,
						TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 1, TimestampMs: 1000},
							{Value: 2, TimestampMs: 2000},
							{Value: 3, TimestampMs: 3000},
							{Value: 4, TimestampMs: 4000},
							{Value: 5, TimestampMs: 5000},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of samples where there is complete overlap.",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3],[4,4],[5,5]]}}}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
											{Value: 4, TimestampMs: 4000},
											{Value: 5, TimestampMs: 5000},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
						TotalQueryableSamples: 14,
						TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 2, TimestampMs: 2000},
							{Value: 3, TimestampMs: 3000},
							{Value: 4, TimestampMs: 4000},
							{Value: 5, TimestampMs: 5000},
						},
					}},
				},
			},
		}, {
			name: "[stats] Peak samples should be the largest one among the responses",
			input: []tripperware.Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3]],"peakSamples":20}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3],[4,4],[5,5]],"peakSamples":22}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[6,"6"],[7,"7"],[8,"8"],[9,"9"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[6,6],[7,7],[8,8],[9,9]],"peakSamples":25}}}}`),
			},
			expected: &tripperware.PrometheusResponse{
				Status: StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: matrix,
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
										Samples: []cortexpb.Sample{
											{Value: 2, TimestampMs: 2000},
											{Value: 3, TimestampMs: 3000},
											{Value: 4, TimestampMs: 4000},
											{Value: 5, TimestampMs: 5000},
											{Value: 6, TimestampMs: 6000},
											{Value: 7, TimestampMs: 7000},
											{Value: 8, TimestampMs: 8000},
											{Value: 9, TimestampMs: 9000},
										},
									},
								},
							},
						},
					},
					Stats: &tripperware.PrometheusResponseStats{Samples: &tripperware.PrometheusResponseSamplesStats{
						TotalQueryableSamples: 44,
						TotalQueryableSamplesPerStep: []*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 2, TimestampMs: 2000},
							{Value: 3, TimestampMs: 3000},
							{Value: 4, TimestampMs: 4000},
							{Value: 5, TimestampMs: 5000},
							{Value: 6, TimestampMs: 6000},
							{Value: 7, TimestampMs: 7000},
							{Value: 8, TimestampMs: 8000},
							{Value: 9, TimestampMs: 9000},
						},
						PeakSamples: 25,
					}},
				},
			},
		}} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancelCtx := context.WithCancel(context.Background())
			if tc.cancelCtx {
				cancelCtx()
			}
			output, err := PrometheusCodec.MergeResponse(ctx, nil, tc.input...)
			require.Equal(t, tc.expectedErr, err)
			if err != nil {
				cancelCtx()
				return
			}
			require.Equal(t, tc.expected, output)
			cancelCtx()
		})
	}
}

func TestGzippedResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		body   string
		status int
		err    error
	}{
		{
			body:   `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3]],"peakSamples":10}}}}`,
			status: 200,
		},
		{
			body:   `error generic 400`,
			status: 400,
			err:    httpgrpc.Errorf(400, `error generic 400`),
		},
		{
			status: 400,
			err:    httpgrpc.Errorf(400, ""),
		},
	} {
		for _, c := range []bool{true, false} {
			c := c
			t.Run(fmt.Sprintf("compressed %t [%s]", c, tc.body), func(t *testing.T) {
				t.Parallel()
				h := http.Header{
					"Content-Type": []string{"application/json"},
				}

				responseBody := bytes.NewBuffer([]byte(tc.body))
				if c {
					h.Set("Content-Encoding", "gzip")
					var buf bytes.Buffer
					w := gzip.NewWriter(&buf)
					_, err := w.Write([]byte(tc.body))
					require.NoError(t, err)
					w.Close()
					responseBody = &buf
				}

				response := &http.Response{
					StatusCode: tc.status,
					Header:     h,
					Body:       io.NopCloser(responseBody),
				}
				r, err := PrometheusCodec.DecodeResponse(context.Background(), response, nil)
				require.Equal(t, tc.err, err)

				if err == nil {
					resp, err := json.Marshal(r)
					require.NoError(t, err)

					require.Equal(t, tc.body, string(resp))
				}
			})
		}
	}
}

func mustParse(t *testing.T, response string) tripperware.Response {
	var resp tripperware.PrometheusResponse
	// Needed as goimports automatically add a json import otherwise.
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	require.NoError(t, json.Unmarshal([]byte(response), &resp))
	return &resp
}
