package stats

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/querier/series"
)

func TestSeriesSetTracker_ShouldTrackStatsOnIterationEnd(t *testing.T) {
	set := series.NewConcreteSeriesSet([]storage.Series{
		series.NewEmptySeries(labels.New(labels.Label{
			Name:  labels.MetricName,
			Value: "metric_1",
		})),
		series.NewEmptySeries(labels.New(labels.Label{
			Name:  labels.MetricName,
			Value: "metric_2",
		})),
		series.NewEmptySeries(labels.New(labels.Label{
			Name:  labels.MetricName,
			Value: "metric_2",
		})),
	})

	// Wrap the original series set with the tracker.
	stats := Stats{}
	set = NewSeriesSetTracker(set, &stats)

	for set.Next() {
		require.Equal(t, int32(0), stats.GetSeries())
	}

	require.NoError(t, set.Err())
	require.Equal(t, int32(3), stats.GetSeries())

	// Another call to Next() shouldn't cause stats to be double counted.
	require.False(t, set.Next())
	require.Equal(t, int32(3), stats.GetSeries())
}
