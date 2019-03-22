package ingester

import (
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

// Test forSeriesMatching correctly batches up series.
func TestForSeriesMatchingBatching(t *testing.T) {
	for _, tc := range []struct {
		numSeries, batchSize int
	}{
		{100, 10},
		{99, 10},
		{98, 10},
		{5, 10},
		{10, 1},
		{1, 1},
	} {
		t.Run(fmt.Sprintf("numSeries=%d,batchSize=%d", tc.numSeries, tc.batchSize), func(t *testing.T) {
			_, ing := newDefaultTestStore(t)
			userIDs, _ := pushTestSamples(t, ing, tc.numSeries, 100)

			for _, userID := range userIDs {
				ctx := user.InjectOrgID(context.Background(), userID)
				instance, ok, err := ing.userStates.getViaContext(ctx)
				require.NoError(t, err)
				require.True(t, ok)

				matcher, err := labels.NewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")
				require.NoError(t, err)

				total, batch, batches := 0, 0, 0
				err = instance.forSeriesMatching(ctx, []*labels.Matcher{matcher},
					func(_ context.Context, _ model.Fingerprint, s *memorySeries) error {
						total++
						batch++
						return nil
					},
					func(context.Context) error {
						require.True(t, batch <= tc.batchSize)
						batch = 0
						batches++
						return nil
					},
					tc.batchSize)
				require.NoError(t, err)
				require.Equal(t, tc.numSeries, total)
				require.Equal(t, int(math.Ceil(float64(tc.numSeries)/float64(tc.batchSize))), batches)
			}
		})
	}
}
