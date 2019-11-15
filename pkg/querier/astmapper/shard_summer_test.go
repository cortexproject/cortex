package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestShardSummer(t *testing.T) {
	var testExpr = []struct {
		shards   int
		input    string
		expected string
	}{
		{
			shards: 3,
			input:  `sum(rate(bar1{baz="blip"}[1m]))`,
			expected: `sum without(__cortex_shard__) (
			  sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
			  sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
			  sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
			)`,
		},
		{
			shards: 3,
			input:  `sum by(foo) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum by(foo) (
			  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
			  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
			  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
			)`,
		},
		{
			shards: 2,
			input: `sum(
				sum by (foo) (rate(bar1{baz="blip"}[1m]))
				/
				sum by (foo) (rate(foo{baz="blip"}[1m]))
			)`,
			expected: `sum(
			  sum by(foo) (
				sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			  )
			  /
			  sum by(foo) (
				sum by(foo, __cortex_shard__) (rate(foo{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo, __cortex_shard__) (rate(foo{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			  )
			)`,
		},
		// This nested sum example is nonsensical, but equivalent.
		{
			shards: 2,
			input:  `sum(sum by(foo) (rate(bar1{baz="blip"}[1m])))`,
			expected: `sum without(__cortex_shard__) (
			  sum by(__cortex_shard__) (
				sum by(foo) (
				  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
				)
			  ) or
			  sum by(__cortex_shard__)(
				sum by(foo) (
				  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
				  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
				)
			  )
			)`,
		},
		// without
		{
			shards: 2,
			input:  `sum without(foo) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum without(__cortex_shard__) (
			  sum without(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum without(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)`,
		},
		// multiple dimensions
		{
			shards: 2,
			input:  `sum by(foo, bom) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum by(foo, bom) (
			  sum by(foo, bom, __cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum by(foo, bom, __cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {

			summer, err := NewShardSummer(c.shards, OrSquasher)
			require.Nil(t, err)
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

func TestShardSummerWithEncoding(t *testing.T) {
	for i, c := range []struct {
		shards   int
		input    string
		expected string
	}{
		{
			shards:   3,
			input:    `sum(rate(bar1{baz="blip"}[1m]))`,
			expected: `sum without(__cortex_shard__) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum by(__cortex_shard__) (rate(bar1{__cortex_shard__=\\\"0_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum by(__cortex_shard__) (rate(bar1{__cortex_shard__=\\\"1_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum by(__cortex_shard__) (rate(bar1{__cortex_shard__=\\\"2_of_3\\\",baz=\\\"blip\\\"}[1m]))\"]}"})`,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer, err := NewShardSummer(c.shards, VectorSquasher)
			require.Nil(t, err)
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

func TestParseShard(t *testing.T) {
	var testExpr = []struct {
		input  string
		output ShardAnnotation
		err    bool
	}{
		{
			input:  "lsdjf",
			output: ShardAnnotation{},
			err:    true,
		},
		{
			input:  "a_of_3",
			output: ShardAnnotation{},
			err:    true,
		},
		{
			input:  "3_of_3",
			output: ShardAnnotation{},
			err:    true,
		},
		{
			input: "1_of_2",
			output: ShardAnnotation{
				Shard: 1,
				Of:    2,
			},
		},
	}

	for _, c := range testExpr {
		t.Run(fmt.Sprint(c.input), func(t *testing.T) {
			shard, err := ParseShard(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.output, shard)
			}
		})
	}

}

func TestShardFromMatchers(t *testing.T) {
	var testExpr = []struct {
		input []*labels.Matcher
		shard *ShardAnnotation
		idx   int
		err   bool
	}{
		{
			input: []*labels.Matcher{
				{},
				{
					Name: ShardLabel,
					Type: labels.MatchEqual,
					Value: ShardAnnotation{
						Shard: 10,
						Of:    16,
					}.String(),
				},
				{},
			},
			shard: &ShardAnnotation{
				Shard: 10,
				Of:    16,
			},
			idx: 1,
			err: false,
		},
		{
			input: []*labels.Matcher{
				{
					Name:  ShardLabel,
					Type:  labels.MatchEqual,
					Value: "invalid-fmt",
				},
			},
			shard: nil,
			idx:   0,
			err:   true,
		},
		{
			input: []*labels.Matcher{},
			shard: nil,
			idx:   0,
			err:   false,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			shard, idx, err := ShardFromMatchers(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.shard, shard)
				require.Equal(t, c.idx, idx)
			}
		})
	}

}
