package querysharding

import (
	"fmt"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFromValue(t *testing.T) {
	var testExpr = []struct {
		input    promql.Value
		err      bool
		expected []queryrange.SampleStream
	}{
		// string (errors)
		{
			input: promql.String{1, "hi"},
			err:   true,
		},
		// Scalar
		{
			input: promql.Scalar{1, 1},
			err:   false,
			expected: []queryrange.SampleStream{
				{
					Samples: []client.Sample{
						client.Sample{
							Value:       1,
							TimestampMs: 1,
						},
					},
				},
			},
		},
		// Vector
		{
			input: promql.Vector{
				promql.Sample{
					promql.Point{1, 1},
					labels.Labels{
						{"a", "a1"},
						{"b", "b1"},
					},
				},
				promql.Sample{
					promql.Point{2, 2},
					labels.Labels{
						{"a", "a2"},
						{"b", "b2"},
					},
				},
			},
			err: false,
			expected: []queryrange.SampleStream{
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
				{
					Labels: []client.LabelAdapter{
						{"a", "a2"},
						{"b", "b2"},
					},
					Samples: []client.Sample{
						client.Sample{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
			},
		},
		// Matrix
		{
			input: promql.Matrix{
				{
					Metric: labels.Labels{
						{"a", "a1"},
						{"b", "b1"},
					},
					Points: []promql.Point{
						{1, 1},
						{2, 2},
					},
				},
				{
					Metric: labels.Labels{
						{"a", "a2"},
						{"b", "b2"},
					},
					Points: []promql.Point{
						{1, 8},
						{2, 9},
					},
				},
			},
			err: false,
			expected: []queryrange.SampleStream{
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
						client.Sample{
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
						client.Sample{
							Value:       8,
							TimestampMs: 1,
						},
						client.Sample{
							Value:       9,
							TimestampMs: 2,
						},
					},
				},
			},
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			result, err := FromValue(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.expected, result)
			}
		})
	}
}
