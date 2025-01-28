package ruler

import (
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestProtoDecode(t *testing.T) {
	tests := []struct {
		description     string
		resp            *tripperware.PrometheusResponse
		expectedVector  promql.Vector
		expectedWarning []string
		expectedErr     error
	}{
		{
			description: "vector",
			resp: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "vector",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Vector{
							Vector: &tripperware.Vector{
								Samples: []tripperware.Sample{
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("foo", "bar")),
										Sample: &cortexpb.Sample{
											Value:       1.234,
											TimestampMs: 1724146338123,
										},
									},
								},
							},
						},
					},
				},
				Warnings: []string{"a", "b", "c"},
			},
			expectedVector: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1724146338123,
					F:      1.234,
				},
			},
			expectedWarning: []string{"a", "b", "c"},
		},
		{
			description: "vector with raw histogram",
			resp: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "vector",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Vector{
							Vector: &tripperware.Vector{
								Samples: []tripperware.Sample{
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromStrings("foo", "bar")),
										RawHistogram: &cortexpb.Histogram{
											Count:          &cortexpb.Histogram_CountFloat{CountFloat: 10},
											Sum:            20,
											Schema:         2,
											ZeroThreshold:  0.001,
											ZeroCount:      &cortexpb.Histogram_ZeroCountFloat{ZeroCountFloat: 12},
											NegativeSpans:  []cortexpb.BucketSpan{{Offset: 2, Length: 2}},
											NegativeCounts: []float64{2, 1},
											PositiveSpans:  []cortexpb.BucketSpan{{Offset: 3, Length: 2}, {Offset: 1, Length: 3}},
											PositiveCounts: []float64{1, 2, 2, 1, 1},
											TimestampMs:    1724146338123,
										},
									},
								},
							},
						},
					},
				},
				Warnings: []string{"a", "b", "c"},
			},
			expectedVector: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1724146338123,
					H: &histogram.FloatHistogram{
						Schema:          2,
						ZeroThreshold:   0.001,
						ZeroCount:       12,
						Count:           10,
						Sum:             20,
						PositiveSpans:   []histogram.Span{{Offset: 3, Length: 2}, {Offset: 1, Length: 3}},
						NegativeSpans:   []histogram.Span{{Offset: 2, Length: 2}},
						PositiveBuckets: []float64{1, 2, 2, 1, 1},
						NegativeBuckets: []float64{2, 1},
						CustomValues:    nil,
					},
				},
			},
			expectedWarning: []string{"a", "b", "c"},
		},
		{
			description: "matrix",
			resp: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "matrix",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{},
					},
				},
				Warnings: []string{"a", "b", "c"},
			},
			expectedVector:  nil,
			expectedWarning: []string{"a", "b", "c"},
			expectedErr:     errors.New("rule result is not a vector or scalar"),
		},
		{
			description: "scalar",
			resp: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "scalar",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_RawBytes{
							RawBytes: []byte(`{"resultType":"scalar","result":[1724146338.123,"1.234"]}`),
						},
					},
				},
				Warnings: []string{"a", "b", "c"},
			},
			expectedVector: promql.Vector{
				{
					Metric: labels.EmptyLabels(),
					T:      1724146338123,
					F:      1.234,
				},
			},
			expectedWarning: []string{"a", "b", "c"},
		},
		{
			description: "string",
			resp: &tripperware.PrometheusResponse{
				Status: "success",
				Data: tripperware.PrometheusData{
					ResultType: "string",
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_RawBytes{
							RawBytes: []byte(`{"resultType":"string","result":[1724146338.123,"1.234"]}`),
						},
					},
				},
				Warnings: []string{"a", "b", "c"},
			},
			expectedVector:  nil,
			expectedWarning: []string{"a", "b", "c"},
			expectedErr:     errors.New("rule result is not a vector or scalar"),
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			b, err := test.resp.Marshal()
			require.NoError(t, err)

			vector, _, err := protobufDecoder.Decode(b)
			require.Equal(t, test.expectedErr, err)
			require.Equal(t, test.expectedVector, vector)
			require.Equal(t, test.expectedWarning, test.resp.Warnings)
		})
	}
}

func TestJsonDecode(t *testing.T) {
	tests := []struct {
		description     string
		body            string
		expectedVector  promql.Vector
		expectedWarning Warnings
		expectedErr     error
	}{
		{
			description: "empty vector",
			body: `{
					"status": "success",
					"data": {"resultType":"vector","result":[]}
				}`,
			expectedVector:  promql.Vector{},
			expectedErr:     nil,
			expectedWarning: nil,
		},
		{
			description: "vector with series",
			body: `{
					"status": "success",
					"data": {
						"resultType": "vector",
						"result": [
							{
								"metric": {"foo":"bar"},
								"value": [1724146338.123,"1.234"]
							},
							{
								"metric": {"bar":"baz"},
								"value": [1724146338.456,"5.678"]
							}
						]
					}
				}`,
			expectedVector: promql.Vector{
				{
					Metric: labels.FromStrings("foo", "bar"),
					T:      1724146338123,
					F:      1.234,
				},
				{
					Metric: labels.FromStrings("bar", "baz"),
					T:      1724146338456,
					F:      5.678,
				},
			},
			expectedErr:     nil,
			expectedWarning: nil,
		},
		{
			description: "scalar",
			body: `{
					"status": "success",
					"data": {"resultType":"scalar","result":[1724146338.123,"1.234"]}
				}`,
			expectedVector: promql.Vector{
				{
					Metric: labels.EmptyLabels(),
					T:      1724146338123,
					F:      1.234,
				},
			},
			expectedErr:     nil,
			expectedWarning: nil,
		},
		{
			description: "matrix",
			body: `{
					"status": "success",
					"data": {"resultType":"matrix","result":[]}
				}`,
			expectedVector:  nil,
			expectedErr:     errors.New("rule result is not a vector or scalar"),
			expectedWarning: nil,
		},
		{
			description: "string",
			body: `{
					"status": "success",
					"data": {"resultType":"string","result":[1724146338.123,"string"]}
				}`,
			expectedVector:  nil,
			expectedErr:     errors.New("rule result is not a vector or scalar"),
			expectedWarning: nil,
		},
		{
			description: "error with warnings",
			body: `{
					"status": "error",
					"errorType": "errorExec",
					"error": "something wrong",
					"warnings": ["a","b","c"]
				}`,
			expectedVector:  nil,
			expectedErr:     errors.New("failed to execute query with error: something wrong"),
			expectedWarning: []string{"a", "b", "c"},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			vector, warning, err := jsonDecoder.Decode([]byte(test.body))
			require.Equal(t, test.expectedVector, vector)
			require.Equal(t, test.expectedWarning, warning)
			require.Equal(t, test.expectedErr, err)
		})
	}
}
