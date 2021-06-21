package ingester

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Test forSeriesMatching correctly batches up series.
func TestForSeriesMatchingBatching(t *testing.T) {
	matchAllNames, err := labels.NewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")
	require.NoError(t, err)
	// We rely on pushTestSamples() creating jobs "testjob0" and "testjob1" in equal parts
	matchNotJob0, err := labels.NewMatcher(labels.MatchNotEqual, model.JobLabel, "testjob0")
	require.NoError(t, err)
	matchNotJob1, err := labels.NewMatcher(labels.MatchNotEqual, model.JobLabel, "testjob1")
	require.NoError(t, err)

	for _, tc := range []struct {
		numSeries, batchSize int
		matchers             []*labels.Matcher
		expected             int
	}{
		{100, 10, []*labels.Matcher{matchAllNames}, 100},
		{99, 10, []*labels.Matcher{matchAllNames}, 99},
		{98, 10, []*labels.Matcher{matchAllNames}, 98},
		{5, 10, []*labels.Matcher{matchAllNames}, 5},
		{10, 1, []*labels.Matcher{matchAllNames}, 10},
		{1, 1, []*labels.Matcher{matchAllNames}, 1},
		{10, 10, []*labels.Matcher{matchAllNames, matchNotJob0}, 5},
		{10, 10, []*labels.Matcher{matchAllNames, matchNotJob1}, 5},
		{100, 10, []*labels.Matcher{matchAllNames, matchNotJob0}, 50},
		{100, 10, []*labels.Matcher{matchAllNames, matchNotJob1}, 50},
		{99, 10, []*labels.Matcher{matchAllNames, matchNotJob0}, 49},
		{99, 10, []*labels.Matcher{matchAllNames, matchNotJob1}, 50},
	} {
		t.Run(fmt.Sprintf("numSeries=%d,batchSize=%d,matchers=%s", tc.numSeries, tc.batchSize, tc.matchers), func(t *testing.T) {
			_, ing := newDefaultTestStore(t)
			userIDs, _ := pushTestSamples(t, ing, tc.numSeries, 100, 0)

			for _, userID := range userIDs {
				ctx := user.InjectOrgID(context.Background(), userID)
				instance, ok, err := ing.userStates.getViaContext(ctx)
				require.NoError(t, err)
				require.True(t, ok)

				total, batch, batches := 0, 0, 0
				err = instance.forSeriesMatching(ctx, tc.matchers,
					func(_ context.Context, _ model.Fingerprint, s *memorySeries) error {
						batch++
						return nil
					},
					func(context.Context) error {
						require.True(t, batch <= tc.batchSize)
						total += batch
						batch = 0
						batches++
						return nil
					},
					tc.batchSize)
				require.NoError(t, err)
				require.Equal(t, tc.expected, total)
				require.Equal(t, int(math.Ceil(float64(tc.expected)/float64(tc.batchSize))), batches)
			}
		})
	}
}

// TestTeardown ensures metrics are updated correctly if the userState is discarded
func TestTeardown(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	_, ing := newTestStore(t,
		defaultIngesterTestConfig(),
		defaultClientTestConfig(),
		defaultLimitsTestConfig(),
		reg)

	pushTestSamples(t, ing, 100, 100, 0)

	// Assert exported metrics (3 blocks, 5 files per block, 2 files WAL).
	metricNames := []string{
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_active_series",
	}

	ing.userStatesMtx.Lock()
	ing.userStates.purgeAndUpdateActiveSeries(time.Now().Add(-5 * time.Minute))
	ing.userStatesMtx.Unlock()

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
			# TYPE cortex_ingester_memory_series_removed_total counter
			cortex_ingester_memory_series_removed_total{user="1"} 0
			cortex_ingester_memory_series_removed_total{user="2"} 0
			cortex_ingester_memory_series_removed_total{user="3"} 0
			# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
			# TYPE cortex_ingester_memory_series_created_total counter
			cortex_ingester_memory_series_created_total{user="1"} 100
			cortex_ingester_memory_series_created_total{user="2"} 100
			cortex_ingester_memory_series_created_total{user="3"} 100
			# HELP cortex_ingester_memory_series The current number of series in memory.
			# TYPE cortex_ingester_memory_series gauge
			cortex_ingester_memory_series 300
			# HELP cortex_ingester_memory_users The current number of users in memory.
			# TYPE cortex_ingester_memory_users gauge
			cortex_ingester_memory_users 3
			# HELP cortex_ingester_active_series Number of currently active series per user.
			# TYPE cortex_ingester_active_series gauge
			cortex_ingester_active_series{user="1"} 100
			cortex_ingester_active_series{user="2"} 100
			cortex_ingester_active_series{user="3"} 100
		`), metricNames...))

	ing.userStatesMtx.Lock()
	defer ing.userStatesMtx.Unlock()
	ing.userStates.teardown()

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
	# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
	# TYPE cortex_ingester_memory_series_removed_total counter
	cortex_ingester_memory_series_removed_total{user="1"} 100
	cortex_ingester_memory_series_removed_total{user="2"} 100
	cortex_ingester_memory_series_removed_total{user="3"} 100
	# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
	# TYPE cortex_ingester_memory_series_created_total counter
	cortex_ingester_memory_series_created_total{user="1"} 100
	cortex_ingester_memory_series_created_total{user="2"} 100
	cortex_ingester_memory_series_created_total{user="3"} 100
	# HELP cortex_ingester_memory_series The current number of series in memory.
	# TYPE cortex_ingester_memory_series gauge
	cortex_ingester_memory_series 0
	# HELP cortex_ingester_memory_users The current number of users in memory.
	# TYPE cortex_ingester_memory_users gauge
	cortex_ingester_memory_users 0
	# HELP cortex_ingester_active_series Number of currently active series per user.
	# TYPE cortex_ingester_active_series gauge
	cortex_ingester_active_series{user="1"} 0
	cortex_ingester_active_series{user="2"} 0
	cortex_ingester_active_series{user="3"} 0
	`), metricNames...))
}

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
