package discardedseries

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestPerLabelsetDiscardedSeriesTracker(t *testing.T) {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cortex_discarded_series_per_labelset",
			Help: "The number of series that include discarded samples for each labelset.",
		},
		[]string{"reason", "user", "labelset"},
	)

	tracker := NewDiscardedSeriesPerLabelsetTracker(gauge)
	user1 := "user1"
	user2 := "user2"
	series1 := labels.FromStrings("__name__", "1")
	series2 := labels.FromStrings("__name__", "2")
	labelset1 := uint64(10)
	labelset2 := uint64(20)
	labelset3 := uint64(30)
	labelsetId1 := "ten"
	labelsetId2 := "twenty"
	labelsetId3 := "thirty"

	tracker.Track(user1, series1.Hash(), labelset1, labelsetId1)
	tracker.Track(user1, series1.Hash(), labelset2, labelsetId2)

	tracker.Track(user2, series1.Hash(), labelset1, labelsetId1)
	tracker.Track(user2, series1.Hash(), labelset1, labelsetId1)
	tracker.Track(user2, series1.Hash(), labelset1, labelsetId1)
	tracker.Track(user2, series2.Hash(), labelset1, labelsetId1)

	require.Equal(t, tracker.getSeriesCount(user1, labelset1), 1)
	require.Equal(t, tracker.getSeriesCount(user1, labelset2), 1)
	require.Equal(t, tracker.getSeriesCount(user1, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId1, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId2, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId3, 0)

	require.Equal(t, tracker.getSeriesCount(user2, labelset1), 2)
	require.Equal(t, tracker.getSeriesCount(user2, labelset2), 0)
	require.Equal(t, tracker.getSeriesCount(user2, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId1, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId2, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId3, 0)

	tracker.UpdateMetrics()

	tracker.Track(user1, series1.Hash(), labelset1, labelsetId1)
	tracker.Track(user1, series1.Hash(), labelset1, labelsetId1)

	require.Equal(t, tracker.getSeriesCount(user1, labelset1), 1)
	require.Equal(t, tracker.getSeriesCount(user1, labelset2), 0)
	require.Equal(t, tracker.getSeriesCount(user1, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId1, 1)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId2, 1)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId3, 0)

	require.Equal(t, tracker.getSeriesCount(user2, labelset1), 0)
	require.Equal(t, tracker.getSeriesCount(user2, labelset2), 0)
	require.Equal(t, tracker.getSeriesCount(user2, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId1, 2)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId2, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId3, 0)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(user1, labelset1), 0)
	require.Equal(t, tracker.getSeriesCount(user1, labelset2), 0)
	require.Equal(t, tracker.getSeriesCount(user1, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId1, 1)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId2, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId3, 0)

	require.Equal(t, tracker.getSeriesCount(user2, labelset1), 0)
	require.Equal(t, tracker.getSeriesCount(user2, labelset2), 0)
	require.Equal(t, tracker.getSeriesCount(user2, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId1, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId2, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId3, 0)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(user1, labelset1), 0)
	require.Equal(t, tracker.getSeriesCount(user1, labelset2), 0)
	require.Equal(t, tracker.getSeriesCount(user1, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId1, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId2, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user1, labelsetId3, 0)

	require.Equal(t, tracker.getSeriesCount(user2, labelset1), 0)
	require.Equal(t, tracker.getSeriesCount(user2, labelset2), 0)
	require.Equal(t, tracker.getSeriesCount(user2, labelset3), 0)

	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId1, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId2, 0)
	comparePerLabelsetSeriesVendedCount(t, gauge, user2, labelsetId3, 0)
}

func comparePerLabelsetSeriesVendedCount(t *testing.T, gaugeVec *prometheus.GaugeVec, user string, labelsetLimitId string, val int) {
	gauge, _ := gaugeVec.GetMetricWithLabelValues("per_labelset_series_limit", user, labelsetLimitId)
	require.Equal(t, float64(val), testutil.ToFloat64(gauge))
}
