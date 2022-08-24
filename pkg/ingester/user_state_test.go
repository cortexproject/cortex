package ingester

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestMetricCounter(t *testing.T) {
	const metric = "metric"

	for name, tc := range map[string]struct {
		ignored                   []string
		localLimit                int
		series                    int
		expectedErrorOnLastSeries error
	}{
		"no ignored metrics, limit not reached": {
			ignored:                   nil,
			localLimit:                5,
			series:                    5,
			expectedErrorOnLastSeries: nil,
		},

		"no ignored metrics, limit reached": {
			ignored:                   nil,
			localLimit:                5,
			series:                    6,
			expectedErrorOnLastSeries: errMaxSeriesPerMetricLimitExceeded,
		},

		"ignored metric, limit not reached": {
			ignored:                   []string{metric},
			localLimit:                5,
			series:                    5,
			expectedErrorOnLastSeries: nil,
		},

		"ignored metric, over limit": {
			ignored:                   []string{metric},
			localLimit:                5,
			series:                    10,
			expectedErrorOnLastSeries: nil, // this metric is not checked for series-count.
		},

		"ignored different metric, limit not reached": {
			ignored:                   []string{"another_metric1", "another_metric2"},
			localLimit:                5,
			series:                    5,
			expectedErrorOnLastSeries: nil,
		},

		"ignored different metric, over limit": {
			ignored:                   []string{"another_metric1", "another_metric2"},
			localLimit:                5,
			series:                    6,
			expectedErrorOnLastSeries: errMaxSeriesPerMetricLimitExceeded,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var ignored map[string]struct{}
			if tc.ignored != nil {
				ignored = make(map[string]struct{})
				for _, v := range tc.ignored {
					ignored[v] = struct{}{}
				}
			}

			limits := validation.Limits{MaxLocalSeriesPerMetric: tc.localLimit}

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// We're testing code that's not dependant on sharding strategy, replication factor, etc. To simplify the test,
			// we use local limit only.
			limiter := NewLimiter(overrides, nil, util.ShardingStrategyDefault, true, 3, false)
			mc := newMetricCounter(limiter, ignored)

			for i := 0; i < tc.series; i++ {
				err := mc.canAddSeriesFor("user", metric)
				if i < tc.series-1 {
					assert.NoError(t, err)
					mc.increaseSeriesForMetric(metric)
				} else {
					assert.Equal(t, tc.expectedErrorOnLastSeries, err)
				}
			}
		})
	}
}
