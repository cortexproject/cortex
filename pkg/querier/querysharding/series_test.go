package querysharding

import (
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestResponseToSeries(t *testing.T) {
	var testExpr = []struct {
		name     string
		input    queryrange.Response
		err      bool
		expected *downstreamSeriesSet
	}{
		{
			name: "vector coercion",
			input: queryrange.Response{
				ResultType: promql.ValueTypeVector,
				Result: []queryrange.SampleStream{
					{
						Labels: []client.LabelAdapter{
							{"a", "a1"},
							{"b", "b1"},
						},
						Samples: []client.Sample{
							client.Sample{
								Value:       1,
								TimestampMs: 1,
							},
						},
					},
				},
			},
			err: false,
			expected: &downstreamSeriesSet{
				set: []*downstreamSeries{
					{
						metric: []labels.Label{
							{"a", "a1"},
							{"b", "b1"},
						},
						points: []promql.Point{
							{1, 1},
						},
					},
				},
			},
		},
		{
			name: "matrix coercion",
			input: queryrange.Response{
				ResultType: promql.ValueTypeMatrix,
				Result: []queryrange.SampleStream{
					{
						Labels: []client.LabelAdapter{
							{"a", "a1"},
							{"b", "b1"},
						},
						Samples: []client.Sample{
							{
								Value:       1,
								TimestampMs: 1,
							},
							{
								Value:       2,
								TimestampMs: 2,
							},
						},
					},
					{
						Labels: []client.LabelAdapter{
							{"a", "a2"},
							{"b", "b2"},
						},
						Samples: []client.Sample{
							{
								Value:       8,
								TimestampMs: 1,
							},
							{
								Value:       9,
								TimestampMs: 2,
							},
						},
					},
				},
			},
			err: false,
			expected: &downstreamSeriesSet{
				set: []*downstreamSeries{
					{
						metric: labels.Labels{
							{"a", "a1"},
							{"b", "b1"},
						},
						points: []promql.Point{
							{1, 1},
							{2, 2},
						},
					},
					{
						metric: labels.Labels{
							{"a", "a2"},
							{"b", "b2"},
						},
						points: []promql.Point{
							{1, 8},
							{2, 9},
						},
					},
				},
			}},
		{
			name: "unknown type -- error converting",
			input: queryrange.Response{
				ResultType: "unsupported",
				Result:     []queryrange.SampleStream{},
			},
			err:      true,
			expected: nil,
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			result, err := ResponseToSeries(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.expected, result)
			}
		})
	}
}

func Test_downstreamSeriesSet_impls_storage_SeriesSet(t *testing.T) {
	set := &downstreamSeriesSet{
		// use nil for test expediency -- just checking that indices are maintained
		set: []*downstreamSeries{
			&downstreamSeries{
				metric: labels.Labels{{"a", "a"}},
			},
			&downstreamSeries{
				metric: labels.Labels{{"b", "b"}},
			},
			&downstreamSeries{
				metric: labels.Labels{{"c", "c"}},
			},
		},
	}

	for i := 0; i < len(set.set)-1; i++ {
		require.Nil(t, set.Err())
		require.Equal(t, set.At(), set.set[i])
		require.Equal(t, set.Next(), true)
	}

	require.Equal(t, set.Next(), false)
	require.NotNil(t, set.Err())
}

func Test_downstreamSeries_impls_storage_Series(t *testing.T) {
	series := initSeries()

	// iterator is a passthrough (downstreamSeries impls both Series & SeriesIterator)
	require.Equal(t, series, series.Iterator())

	require.Equal(t, series.Labels(), series.metric)
}

func Test_downstreamSeries_impls_storage_SeriesIterator_Err(t *testing.T) {
	var testExpr = []struct {
		name  string
		input *downstreamSeries
		f     func(*testing.T, *downstreamSeries)
	}{
		{
			"empty",
			&downstreamSeries{},
			func(t *testing.T, series *downstreamSeries) {
				require.NotNil(t, series.Err())
			},
		},
		{
			"past bounds",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				//advance internal index to end of data
				series.i = len(series.points)

				require.NotNil(t, series.Err())
			},
		},
		{
			"past bounds via next",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				for i := 0; i < len(series.points); i++ {
					series.Next()
				}

				require.NotNil(t, series.Err())
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			c.f(t, c.input)
		})
	}
}

func Test_downstreamSeries_impls_storage_SeriesIterator_Next(t *testing.T) {
	var testExpr = []struct {
		name  string
		input *downstreamSeries
		f     func(*testing.T, *downstreamSeries)
	}{
		{
			"next advances series",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				for i := 0; i < len(series.points)-1; i++ {
					require.Equal(t, i, series.i)
					require.Equal(t, true, series.Next())
					require.Equal(t, i+1, series.i)
				}
			},
		},
		{
			"next returns false at end of series",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				for i := 0; i < len(series.points)-1; i++ {
					series.Next()
				}

				require.Equal(t, false, series.Next())
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			c.f(t, c.input)
		})
	}
}

func Test_downstreamSeries_impls_storage_SeriesIterator_At(t *testing.T) {
	var testExpr = []struct {
		name  string
		input *downstreamSeries
		f     func(*testing.T, *downstreamSeries)
	}{
		{
			"At tracks current index",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				for i := 0; i < len(series.points); i++ {
					timestamp, val := series.At()
					require.Equal(t, series.points[i].T, timestamp)
					require.Equal(t, series.points[i].V, val)
					series.Next()
				}
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			c.f(t, c.input)
		})
	}
}

func Test_downstreamSeries_impls_storage_SeriesIterator_Seek(t *testing.T) {
	var testExpr = []struct {
		name  string
		input *downstreamSeries
		f     func(*testing.T, *downstreamSeries)
	}{
		{
			"advances iterator at",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				require.Equal(t, true, series.Seek(2))
				ts, _ := series.At()
				require.Equal(t, int64(2), ts)
			},
		},
		{
			"advances iterator after",
			&downstreamSeries{
				metric: labels.Labels{{"a", "a"}},
				points: []promql.Point{
					{1, 1},
					{2, 2},
					{4, 4},
				},
			},
			func(t *testing.T, series *downstreamSeries) {
				require.Equal(t, true, series.Seek(3))
				ts, _ := series.At()
				require.Equal(t, int64(4), ts)
			},
		},
		{
			"doesnt unnecessarily advancer iter",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				require.Equal(t, true, series.Seek(2))
				require.Equal(t, true, series.Seek(2))
				ts, _ := series.At()
				require.Equal(t, int64(2), ts)
			},
		},
		{
			"seek past end",
			initSeries(),
			func(t *testing.T, series *downstreamSeries) {
				require.Equal(t, false, series.Seek(100))
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			c.f(t, c.input)
		})
	}
}

// helper for generating testable series
func initSeries() *downstreamSeries {
	return &downstreamSeries{
		metric: labels.Labels{{"a", "a"}},
		points: []promql.Point{
			{1, 1},
			{2, 2},
			{3, 3},
		},
	}
}
