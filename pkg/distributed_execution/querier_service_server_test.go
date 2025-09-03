package distributed_execution

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/distributed_execution/querierpb"
)

// TestQuerierServer_Series tests series streaming
func TestQuerierServer_Series(t *testing.T) {
	tests := []struct {
		name       string
		setupCache func() *QueryTracker
		request    *querierpb.SeriesRequest
		wantErr    bool
		errMessage string
	}{
		{
			name: "matrix data type success",
			setupCache: func() *QueryTracker {
				cache := NewQueryTracker()
				matrix := promql.Matrix{
					promql.Series{
						Metric: labels.FromStrings("__name__", "foo"),
						Floats: []promql.FPoint{{F: 1, T: 1000}},
					},
					promql.Series{
						Metric: labels.FromStrings("__name__", "bar"),
						Floats: []promql.FPoint{{F: 2, T: 2000}},
					},
				}
				cache.SetComplete(MakeFragmentKey(1, 1), &v1.QueryData{
					ResultType: parser.ValueTypeMatrix,
					Result:     matrix,
				})
				return cache
			},
			request: &querierpb.SeriesRequest{
				QueryID:    1,
				FragmentID: 1,
			},
			wantErr: false,
		},
		{
			name: "vector data type success",
			setupCache: func() *QueryTracker {
				cache := NewQueryTracker()
				vector := promql.Vector{promql.Sample{}}
				cache.SetComplete(MakeFragmentKey(1, 1),
					&v1.QueryData{
						ResultType: parser.ValueTypeVector,
						Result:     vector,
					})
				return cache
			},
			request: &querierpb.SeriesRequest{
				QueryID:    1,
				FragmentID: 1,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.setupCache()
			server := NewQuerierServer(cache)

			mockStream := &mockSeriesServer{}
			err := server.Series(tt.request, mockStream)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestQuerierServer_Next tests next streaming
func TestQuerierServer_Next(t *testing.T) {
	tests := []struct {
		name       string
		setupCache func() *QueryTracker
		request    *querierpb.NextRequest
		wantErr    bool
		errMessage string
	}{
		{
			name: "matrix data type success",
			setupCache: func() *QueryTracker {
				cache := NewQueryTracker()
				matrix := promql.Matrix{
					promql.Series{
						Metric: labels.FromStrings("__name__", "foo"),
						Floats: []promql.FPoint{{F: 1, T: 1000}},
					},
					promql.Series{
						Metric: labels.FromStrings("__name__", "bar"),
						Floats: []promql.FPoint{{F: 2, T: 2000}},
					},
				}
				cache.SetComplete(MakeFragmentKey(1, 1), &v1.QueryData{
					ResultType: parser.ValueTypeMatrix,
					Result:     matrix,
				})
				return cache
			},
			request: &querierpb.NextRequest{
				QueryID:    1,
				FragmentID: 1,
				Batchsize:  BATCHSIZE,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.setupCache()
			server := NewQuerierServer(cache)

			mockStream := &mockNextServer{}
			err := server.Next(tt.request, mockStream)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestFloatHistorgramConversion function is doing conversion correctly
func TestFloatHistogramConversion(t *testing.T) {
	original := &histogram.FloatHistogram{
		Schema:          1,
		ZeroThreshold:   0.001,
		ZeroCount:       2.0,
		Count:           10.0,
		Sum:             100.0,
		PositiveSpans:   []histogram.Span{{Offset: 1, Length: 2}},
		PositiveBuckets: []float64{1.0, 2.0},
		NegativeSpans:   []histogram.Span{{Offset: -2, Length: 1}},
		NegativeBuckets: []float64{-1.0},
	}

	proto := floatHistogramToFloatHistogramProto(original)

	result := floatHistogramProtoToFloatHistogram(*proto)

	assert.Equal(t, original.Schema, result.Schema)
	assert.Equal(t, original.ZeroThreshold, result.ZeroThreshold)
	assert.Equal(t, original.ZeroCount, result.ZeroCount)
	assert.Equal(t, original.Count, result.Count)
	assert.Equal(t, original.Sum, result.Sum)
	assert.Equal(t, original.PositiveSpans, result.PositiveSpans)
	assert.Equal(t, original.PositiveBuckets, result.PositiveBuckets)
	assert.Equal(t, original.NegativeSpans, result.NegativeSpans)
	assert.Equal(t, original.NegativeBuckets, result.NegativeBuckets)
}

// mock implementations for testing
type mockSeriesServer struct {
	querierpb.Querier_SeriesServer
	sent []*querierpb.SeriesBatch
}

func (m *mockSeriesServer) Send(batch *querierpb.SeriesBatch) error {
	m.sent = append(m.sent, batch)
	return nil
}

func (m *mockSeriesServer) Context() context.Context {
	return context.Background()
}

type mockNextServer struct {
	querierpb.Querier_NextServer
	sent []*querierpb.StepVectorBatch
}

func (m *mockNextServer) Send(batch *querierpb.StepVectorBatch) error {
	m.sent = append(m.sent, batch)
	return nil
}

func (m *mockNextServer) Context() context.Context {
	return context.Background()
}
