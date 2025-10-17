package plan_fragments

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
)

// Tests fragmentation of logical plans, verifying that the fragments contain correct metadata.
// Note: The number of fragments is determined by the distributed optimizer's strategy -
// if the optimizer logic changes, this test will need to be updated accordingly.
func TestFragmenter(t *testing.T) {
	type testCase struct {
		name              string
		query             string
		start             time.Time
		end               time.Time
		expectedFragments int
	}

	now := time.Now()
	tests := []testCase{
		{
			name:              "simple logical query plan - no fragmentation",
			query:             "up",
			start:             now,
			end:               now,
			expectedFragments: 1,
		},
		{
			name:              "binary operation with aggregations",
			query:             "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))",
			start:             now,
			end:               now,
			expectedFragments: 3,
		},
		{
			name:              "multiple binary operation with aggregations",
			query:             "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) + sum(rate(http_requests_total{job=\"cache\"}[5m])) + sum(rate(http_requests_total{job=\"db\"}[5m]))",
			start:             now,
			end:               now,
			expectedFragments: 7,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lp, err := distributed_execution.CreateTestLogicalPlan(tc.query, tc.start, tc.end, 0)
			require.NoError(t, err)

			fragmenter := NewPlanFragmenter()
			res, err := fragmenter.Fragment(uint64(1), (*lp).Root())

			require.NoError(t, err)
			require.Equal(t, tc.expectedFragments, len(res))

			// check the metadata of the fragments of binary expressions
			if len(res) == 3 {
				require.Equal(t, []uint64{res[0].FragmentID, res[1].FragmentID}, res[2].ChildIDs)
			}
		})
	}
}
