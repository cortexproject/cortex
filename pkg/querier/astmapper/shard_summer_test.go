package astmapper

import (
	"fmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShardSummer(t *testing.T) {
	var testExpr = []struct {
		shards   int
		input    string
		expected string
	}{
		{
			3,
			`sum by(foo) (rate(bar1{baz="blip"}[1m]))`,
			`sum by(foo) (
			  sum by(foo) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
			  sum by(foo) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
			  sum by(foo) (rate(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
			)`,
		},
		{
			2,
			`sum(
				sum by (foo) (rate(bar1{baz="blip"}[1m]))
				/
				sum by (foo) (rate(foo{baz="blip"}[1m]))
			)`,
			`sum(
			  sum by(foo) (
				sum by(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			  )
			  /
			  sum by(foo) (
				sum by(foo) (rate(foo{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo) (rate(foo{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			  )
			)`,
		},
		// This is currently redundant but still equivalent: sums split into sharded versions, including summed sums.
		{
			2,
			`sum(sum by(foo) (rate(bar1{baz="blip"}[1m])))`,
			`sum(
			  sum(
				sum by(foo) (
				  sum by(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				  sum by(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
				)
			  ) or
			  sum(
				sum by(foo) (
				  sum by(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				  sum by(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
				)
			  )
			)`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer := NewShardSummer(c.shards, nil)
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)

			expected, err := promql.ParseExpr(c.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}
