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
			`sum by(foo) (__embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22305f6f665f33222c62617a3d22626c6970227d5b316d5d2929"} or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22315f6f665f33222c62617a3d22626c6970227d5b316d5d2929"} or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22325f6f665f33222c62617a3d22626c6970227d5b316d5d2929"})`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer := NewShardSummer(c.shards, VectorSquasher)
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)

			require.Equal(t, c.expected, res.String())
		})
	}
}

func TestShallowEmbedSelectors(t *testing.T) {
	var testExpr = []struct {
		input    string
		expected string
	}{
		// already holds embedded query, so noop (don't double encode)
		{
			`sum by(foo) (__embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22305f6f665f33222c62617a3d22626c6970227d5b316d5d2929"} or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22315f6f665f33222c62617a3d22626c6970227d5b316d5d2929"} or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22325f6f665f33222c62617a3d22626c6970227d5b316d5d2929"})`,
			`sum by(foo) (__embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22305f6f665f33222c62617a3d22626c6970227d5b316d5d2929"} or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22315f6f665f33222c62617a3d22626c6970227d5b316d5d2929"} or __embedded_query__{__cortex_query__="73756d20627928666f6f2920287261746528626172317b5f5f636f727465785f73686172645f5f3d22325f6f665f33222c62617a3d22626c6970227d5b316d5d2929"})`,
		},
		{
			`http_requests_total{cluster="prod"}`,
			`__embedded_query__{__cortex_query__="687474705f72657175657374735f746f74616c7b636c75737465723d2270726f64227d"}`,
		},
		{
			`rate(http_requests_total{cluster="eu-west2"}[5m]) or rate(http_requests_total{cluster="us-central1"}[5m])`,
			`rate(__embedded_query__{__cortex_query__="687474705f72657175657374735f746f74616c7b636c75737465723d2265752d7765737432227d5b356d5d"}[1m]) or rate(__embedded_query__{__cortex_query__="687474705f72657175657374735f746f74616c7b636c75737465723d2275732d63656e7472616c31227d5b356d5d"}[1m])`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := ShallowEmbedSelectors.Map(expr)
			require.Nil(t, err)

			require.Equal(t, c.expected, res.String())
		})
	}
}
