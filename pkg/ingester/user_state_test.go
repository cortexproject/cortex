package ingester

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

// Test forSeriesMatching correctly batches up series.
func TestForSeriesMatchingBatching(t *testing.T) {
	_, ing := newDefaultTestStore(t)
	userIDs, _ := pushTestSamples(t, ing, 98, 100)

	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		instance, ok, err := ing.userStates.getViaContext(ctx)
		require.NoError(t, err)
		require.True(t, ok)

		matcher, err := labels.NewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")
		require.NoError(t, err)

		total, batchSize, batchCount := 0, 0, 0
		err = instance.forSeriesMatching(ctx, []*labels.Matcher{matcher},
			func(_ context.Context, _ model.Fingerprint, s *memorySeries) error {
				total++
				batchSize++
				return nil
			},
			func(context.Context) error {
				fmt.Println(batchSize)
				require.True(t, batchSize <= 10)
				batchSize = 0
				batchCount++
				return nil
			},
			10)
		require.NoError(t, err)
		require.Equal(t, 98, total)
		require.Equal(t, 10, batchCount)
	}
}
