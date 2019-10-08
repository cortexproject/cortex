package astmapper

import (
	"fmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSquash(t *testing.T) {
	var testExpr = []struct {
		shards   int
		input    string
		expected string
	}{
		{
			3,
			`sum by(foo) (rate(bar1{baz="blip"}[1m]))`,
			`sum by(foo) (__embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22305f6f665f33222c62617a3d22626c6970227d5b316d5d2929"}[1m] or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22315f6f665f33222c62617a3d22626c6970227d5b316d5d2929"}[1m] or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22325f6f665f33222c62617a3d22626c6970227d5b316d5d2929"}[1m])`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer := NewShardSummer(c.shards, Squash)
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)

			require.Equal(t, c.expected, res.String())
		})
	}
}
