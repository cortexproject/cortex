package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestShallowEmbedSelectors(t *testing.T) {
	var testExpr = []struct {
		input    string
		expected string
	}{
		// already holds embedded queries, so noop (don't double encode)
		{
			`sum by(foo) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"})`,
			`sum by(foo) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"})`,
		},
		{
			`http_requests_total{cluster="prod"}`,
			`__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"}`,
		},
		{
			`rate(http_requests_total{cluster="eu-west2"}[5m]) or rate(http_requests_total{cluster="us-central1"}[5m])`,
			`rate(__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"eu-west2\\\"}[5m]\"]}"}[1m]) or rate(__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"us-central1\\\"}[5m]\"]}"}[1m])`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			mapper := ShallowEmbedSelectors
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := mapper.Map(expr)
			require.Nil(t, err)

			require.Equal(t, c.expected, res.String())
		})
	}
}
