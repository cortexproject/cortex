package ingester

import (
	"fmt"
	"math"
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
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
	matchFirstShard, err := labels.NewMatcher(labels.MatchEqual, astmapper.ShardLabel, astmapper.ShardAnnotation{
		Shard: 0,
		Of:    5,
	}.String())
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
		// shard test
		{100, 10, []*labels.Matcher{matchAllNames, matchFirstShard}, 15},
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
						shard, _, err := astmapper.ShardFromMatchers(tc.matchers)
						require.Nil(t, err)
						// if expected to match a shard, make sure the correct label has been injected
						if shard != nil {
							var found bool
							for _, l := range s.metric {
								if l.Name == astmapper.ShardLabel {
									extracted, err := astmapper.ParseShard(l.Value)
									require.Nil(t, err)
									require.Equal(t, *shard, extracted)
									found = true
								}
							}
							require.True(t, found)
						}
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

func TestMatchesShard(t *testing.T) {
	mkSeries := func(s string) *memorySeries {
		return &memorySeries{
			metric: labels.Labels{
				{Name: s, Value: "filler"},
			},
		}
	}
	for i, tc := range []struct {
		shard    *astmapper.ShardAnnotation
		series   *memorySeries
		expected bool
	}{
		{
			shard: &astmapper.ShardAnnotation{
				Shard: 0,
				Of:    2,
			},
			series:   mkSeries("a"),
			expected: false,
		},
		{
			shard:    nil,
			series:   mkSeries("a"),
			expected: false,
		},
		{
			shard: &astmapper.ShardAnnotation{
				Shard: 0,
				Of:    2,
			},
			series:   mkSeries("succeeds"),
			expected: true,
		},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, tc.expected, matchesShard(tc.shard, tc.series))
		})
	}
}
