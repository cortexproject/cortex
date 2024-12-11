//go:build !race

package ingester

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// Running this test without race check as there is a known prometheus race condition.
// See https://github.com/prometheus/prometheus/pull/15141 and https://github.com/prometheus/prometheus/pull/15316
func TestExpandedCachePostings_Race(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{2 * time.Hour}
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.PostingsCache.Head.Enabled = true

	r := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	wg := sync.WaitGroup{}
	labelNames := 100
	seriesPerLabelName := 200

	for j := 0; j < labelNames; j++ {
		metricName := fmt.Sprintf("test_metric_%d", j)
		wg.Add(seriesPerLabelName * 2)
		for k := 0; k < seriesPerLabelName; k++ {
			go func() {
				defer wg.Done()
				_, err := i.Push(ctx, cortexpb.ToWriteRequest(
					[]labels.Labels{labels.FromStrings(labels.MetricName, metricName, "k", strconv.Itoa(k))},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}}, nil, nil, cortexpb.API))
				require.NoError(t, err)
			}()

			go func() {
				defer wg.Done()
				err := i.QueryStream(&client.QueryRequest{
					StartTimestampMs: 0,
					EndTimestampMs:   math.MaxInt64,
					Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricName}},
				}, &mockQueryStreamServer{ctx: ctx})
				require.NoError(t, err)
			}()
		}

		wg.Wait()

		s := &mockQueryStreamServer{ctx: ctx}
		err = i.QueryStream(&client.QueryRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   math.MaxInt64,
			Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricName}},
		}, s)
		require.NoError(t, err)

		set, err := seriesSetFromResponseStream(s)
		require.NoError(t, err)
		res, err := client.MatrixFromSeriesSet(set)
		require.NoError(t, err)
		require.Equal(t, seriesPerLabelName, res.Len())
	}
}
