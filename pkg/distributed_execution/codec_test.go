package distributed_execution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
)

func TestUnmarshalWithLogicalPlan(t *testing.T) {
	t.Run("unmarshal complex query plan", func(t *testing.T) {
		start := time.Now()
		end := start.Add(1 * time.Hour)
		step := 15 * time.Second

		testCases := []struct {
			name  string
			query string
		}{
			{
				name:  "binary operation",
				query: "http_requests_total + rate(node_cpu_seconds_total[5m])",
			},
			{
				name:  "aggregation",
				query: "sum(rate(http_requests_total[5m])) by (job)",
			},
			{
				name:  "complex query",
				query: "sum(rate(http_requests_total{job='prometheus'}[5m])) by (job) / sum(rate(node_cpu_seconds_total[5m])) by (job)",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				plan, _, err := CreateTestLogicalPlan(tc.query, start, end, step)
				require.NoError(t, err)
				require.NotNil(t, plan)

				data, err := logicalplan.Marshal((*plan).Root())
				require.NoError(t, err)

				node, err := Unmarshal(data)
				require.NoError(t, err)
				require.NotNil(t, node)

				// the logical plan node before and after marshal/unmarshal should be the same
				verifyNodeStructure(t, (*plan).Root(), node)
			})
		}
	})
}

func verifyNodeStructure(t *testing.T, expected logicalplan.Node, actual logicalplan.Node) {
	require.Equal(t, expected.Type(), actual.Type())
	require.Equal(t, expected.String(), actual.String())
	require.Equal(t, expected.ReturnType(), actual.ReturnType())

	expectedChildren := expected.Children()
	actualChildren := actual.Children()

	require.Equal(t, len(expectedChildren), len(actualChildren))

	for i := 0; i < len(expectedChildren); i++ {
		if expectedChildren[i] != nil && actualChildren[i] != nil {
			verifyNodeStructure(t, *expectedChildren[i], *actualChildren[i])
		}
	}
}
