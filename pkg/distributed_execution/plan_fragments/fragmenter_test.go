package plan_fragments

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/logical_plan"
)

func TestFragmenter(t *testing.T) {
	type testCase struct {
		name              string
		query             string
		start             time.Time
		end               time.Time
		expectedFragments int
	}

	now := time.Now()

	// more tests will be added when distributed optimizer and fragmenter are implemented
	tests := []testCase{
		{
			name:              "simple logical query plan - no fragmentation",
			query:             "up",
			start:             now,
			end:               now,
			expectedFragments: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lp, err := logical_plan.CreateTestLogicalPlan(tc.query, tc.start, tc.end, 0)
			require.NoError(t, err)

			fragmenter := NewDummyFragmenter()
			res, err := fragmenter.Fragment((*lp).Root())

			require.NoError(t, err)
			require.Equal(t, tc.expectedFragments, len(res))
		})
	}
}
