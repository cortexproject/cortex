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
		name                 string
		query                string
		start                time.Time
		end                  time.Time
		expectedFragmentsCnt int
	}

	now := time.Now()
	tests := []testCase{
		{
			name:                 "simple logical query plan - no fragmentation",
			query:                "up",
			start:                now,
			end:                  now,
			expectedFragmentsCnt: 1,
		},
		{
			name:                 "binary operation with aggregations",
			query:                "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))",
			start:                now,
			end:                  now,
			expectedFragmentsCnt: 3,
		},
		{
			name:                 "multiple binary operation with aggregations",
			query:                "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) + sum(rate(http_requests_total{job=\"cache\"}[5m]))",
			start:                now,
			end:                  now,
			expectedFragmentsCnt: 5,
		},
		{
			name:                 "multiple binary operation with aggregations",
			query:                "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) + sum(rate(http_requests_total{job=\"cache\"}[5m])) + sum(rate(http_requests_total{job=\"db\"}[5m]))",
			start:                now,
			end:                  now,
			expectedFragmentsCnt: 7,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lp, err := distributed_execution.CreateTestLogicalPlan(tc.query, tc.start, tc.end, 0)
			require.NoError(t, err)

			fragmenter := NewPlanFragmenter()
			res, err := fragmenter.Fragment(uint64(1), (*lp).Root())

			require.NoError(t, err)

			// first check the number of fragments
			require.Equal(t, tc.expectedFragmentsCnt, len(res))

			// check the fragments returned by comparing child IDs, ensuring correct hierarchy
			if len(res) == 3 { // 3 fragment cases
				// current binary split:
				// (due to the design of the distributed optimizer)
				//     2
				//    /  \
				//   0    1
				require.Empty(t, res[0].ChildIDs)
				require.Empty(t, res[1].ChildIDs)
				require.ElementsMatch(t, []uint64{res[0].FragmentID, res[1].FragmentID}, res[2].ChildIDs)
			} else if len(res) == 5 {
				// current binary split:
				// 		   4
				// 		 /   \
				// 		2     3
				// 	  /  \
				//   0    1
				require.Empty(t, res[0].ChildIDs)
				require.Empty(t, res[1].ChildIDs)
				require.Empty(t, res[3].ChildIDs)

				require.Containsf(t, res[2].ChildIDs, res[0].FragmentID, "child ID of fragment 0 not found in layer 2")
				require.Containsf(t, res[2].ChildIDs, res[1].FragmentID, "child ID of fragment 1 not found in layer 2")
				require.Equal(t, len(res[2].ChildIDs), 2) // binary check

				require.Containsf(t, res[4].ChildIDs, res[3].FragmentID, "child ID of fragment 3 not found in layer 3")
				require.Containsf(t, res[4].ChildIDs, res[2].FragmentID, "child ID of fragment 4 not found in layer 3")
				require.Equal(t, len(res[4].ChildIDs), 2) // binary check

			} else if len(res) == 7 { // 7 fragment cases
				// current binary split:
				//         6
				//       /   \
				//      4     5
				//     /  \
				//    2   3
				//   /  \
				//  0    1

				require.Empty(t, res[0].ChildIDs)
				require.Empty(t, res[1].ChildIDs)
				require.Empty(t, res[3].ChildIDs)
				require.Empty(t, res[5].ChildIDs)

				require.Containsf(t, res[2].ChildIDs, res[0].FragmentID, "child ID of fragment 0 not found in layer 2")
				require.Containsf(t, res[2].ChildIDs, res[1].FragmentID, "child ID of fragment 1 not found in layer 2")
				require.Equal(t, len(res[2].ChildIDs), 2) // binary check

				require.Containsf(t, res[4].ChildIDs, res[3].FragmentID, "child ID of fragment 3 not found in layer 3")
				require.Containsf(t, res[4].ChildIDs, res[2].FragmentID, "child ID of fragment 4 not found in layer 3")
				require.Equal(t, len(res[4].ChildIDs), 2) // binary check

				require.Containsf(t, res[6].ChildIDs, res[4].FragmentID, "child ID of fragment 4 not found in layer 4")
				require.Containsf(t, res[6].ChildIDs, res[5].FragmentID, "child ID of fragment 5 not found in layer 4")
				require.Equal(t, len(res[6].ChildIDs), 2) // binary check
			}
		})
	}
}
