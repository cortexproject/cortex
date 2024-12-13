package codec

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

func TestProtobufCodec_Encode(t *testing.T) {
	testFloatHistogram := &histogram.FloatHistogram{
		Schema:        2,
		ZeroThreshold: 0.001,
		ZeroCount:     12,
		Count:         10,
		Sum:           20,
		PositiveSpans: []histogram.Span{
			{Offset: 3, Length: 2},
			{Offset: 1, Length: 3},
		},
		NegativeSpans: []histogram.Span{
			{Offset: 2, Length: 2},
		},
		PositiveBuckets: []float64{1, 2, 2, 1, 1},
		NegativeBuckets: []float64{2, 1},
	}
	testProtoHistogram := cortexpb.FloatHistogramToHistogramProto(1000, testFloatHistogram)

	tests := []struct {
		name           string
		data           *v1.QueryData
		cortexInternal bool
		expected       *tripperware.PrometheusResponse
	}{
		{
			data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					promql.Sample{
						Metric: labels.FromStrings("__name__", "foo"),
						T:      1000,
						F:      1,
					},
					promql.Sample{
						Metric: labels.FromStrings("__name__", "bar"),
						T:      2000,
						F:      2,
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValVector.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Vector{
							Vector: &tripperware.Vector{
								Samples: []tripperware.Sample{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "foo"},
										},
										Sample: &cortexpb.Sample{Value: 1, TimestampMs: 1000},
									},
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "bar"},
										},
										Sample: &cortexpb.Sample{Value: 2, TimestampMs: 2000},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			data: &v1.QueryData{
				ResultType: parser.ValueTypeScalar,
				Result:     promql.Scalar{T: 1000, V: 1},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValScalar.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_RawBytes{
							RawBytes: []byte(`{"resultType":"scalar","result":[1,"1"]}`),
						},
					},
				},
			},
		},
		{
			data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Metric: labels.FromStrings("__name__", "foo"),
						Floats: []promql.FPoint{{F: 1, T: 1000}},
					},
					promql.Series{
						Metric: labels.FromStrings("__name__", "bar"),
						Floats: []promql.FPoint{{F: 2, T: 2000}},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "foo"},
										},
										Samples: []cortexpb.Sample{
											{Value: 1, TimestampMs: 1000},
										},
									},
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "bar"},
										},
										Samples: []cortexpb.Sample{
											{Value: 2, TimestampMs: 2000},
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
			data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 1000}},
						Metric: labels.FromStrings("__name__", "foo"),
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "foo"},
										},
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
			data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Metric: labels.Labels{
							{Name: "__name__", Value: "foo"},
							{Name: "__job__", Value: "bar"},
						},
						Floats: []promql.FPoint{
							{F: 0.14, T: 18555000},
							{F: 2.9, T: 18556000},
							{F: 30, T: 18557000},
						},
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "foo"},
											{Name: "__job__", Value: "bar"},
										},
										Samples: []cortexpb.Sample{
											{Value: 0.14, TimestampMs: 18555000},
											{Value: 2.9, TimestampMs: 18556000},
											{Value: 30, TimestampMs: 18557000},
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
			data: &v1.QueryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Histograms: []promql.HPoint{{H: testFloatHistogram, T: 1000}},
						Metric:     labels.FromStrings("__name__", "foo"),
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Matrix{
							Matrix: &tripperware.Matrix{
								SampleStreams: []tripperware.SampleStream{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "foo"},
										},
										Histograms: []tripperware.SampleHistogramPair{
											{
												TimestampMs: 1000,
												Histogram: tripperware.SampleHistogram{
													Count: 10,
													Sum:   20,
													Buckets: []*tripperware.HistogramBucket{
														{
															Boundaries: 1,
															Upper:      -1.414213562373095,
															Lower:      -1.6817928305074288,
															Count:      1,
														},
														{
															Boundaries: 1,
															Upper:      -1.189207115002721,
															Lower:      -1.414213562373095,
															Count:      2,
														},
														{
															Boundaries: 3,
															Upper:      0.001,
															Lower:      -0.001,
															Count:      12,
														},
														{
															Boundaries: 0,
															Upper:      1.6817928305074288,
															Lower:      1.414213562373095,
															Count:      1,
														},
														{
															Boundaries: 0,
															Upper:      2,
															Lower:      1.6817928305074288,
															Count:      2,
														},
														{
															Boundaries: 0,
															Upper:      2.82842712474619,
															Lower:      2.378414230005442,
															Count:      2,
														},
														{
															Boundaries: 0,
															Upper:      3.3635856610148576,
															Lower:      2.82842712474619,
															Count:      1,
														},
														{
															Boundaries: 0,
															Upper:      4,
															Lower:      3.3635856610148576,
															Count:      1,
														},
													},
												},
											},
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
			data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					promql.Sample{
						Metric: labels.FromStrings("__name__", "foo"),
						T:      1000,
						H:      testFloatHistogram,
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValVector.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Vector{
							Vector: &tripperware.Vector{
								Samples: []tripperware.Sample{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "foo"},
										},
										Histogram: &tripperware.SampleHistogramPair{
											TimestampMs: 1000,
											Histogram: tripperware.SampleHistogram{
												Count: 10,
												Sum:   20,
												Buckets: []*tripperware.HistogramBucket{
													{
														Boundaries: 1,
														Upper:      -1.414213562373095,
														Lower:      -1.6817928305074288,
														Count:      1,
													},
													{
														Boundaries: 1,
														Upper:      -1.189207115002721,
														Lower:      -1.414213562373095,
														Count:      2,
													},
													{
														Boundaries: 3,
														Upper:      0.001,
														Lower:      -0.001,
														Count:      12,
													},
													{
														Boundaries: 0,
														Upper:      1.6817928305074288,
														Lower:      1.414213562373095,
														Count:      1,
													},
													{
														Boundaries: 0,
														Upper:      2,
														Lower:      1.6817928305074288,
														Count:      2,
													},
													{
														Boundaries: 0,
														Upper:      2.82842712474619,
														Lower:      2.378414230005442,
														Count:      2,
													},
													{
														Boundaries: 0,
														Upper:      3.3635856610148576,
														Lower:      2.82842712474619,
														Count:      1,
													},
													{
														Boundaries: 0,
														Upper:      4,
														Lower:      3.3635856610148576,
														Count:      1,
													},
												},
											},
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
			name:           "cortex internal with native histogram",
			cortexInternal: true,
			data: &v1.QueryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					promql.Sample{
						Metric: labels.FromStrings("__name__", "foo"),
						T:      1000,
						H:      testFloatHistogram,
					},
				},
			},
			expected: &tripperware.PrometheusResponse{
				Status: tripperware.StatusSuccess,
				Data: tripperware.PrometheusData{
					ResultType: model.ValVector.String(),
					Result: tripperware.PrometheusQueryResult{
						Result: &tripperware.PrometheusQueryResult_Vector{
							Vector: &tripperware.Vector{
								Samples: []tripperware.Sample{
									{
										Labels: []cortexpb.LabelAdapter{
											{Name: "__name__", Value: "foo"},
										},
										RawHistogram: &testProtoHistogram,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			codec := ProtobufCodec{CortexInternal: test.cortexInternal}
			body, err := codec.Encode(&v1.Response{
				Status: tripperware.StatusSuccess,
				Data:   test.data,
			})
			require.NoError(t, err)
			b, err := proto.Marshal(test.expected)
			require.NoError(t, err)
			require.Equal(t, string(b), string(body))
		})
	}
}
