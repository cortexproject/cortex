package discardedseries

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestLabelSetTracker(t *testing.T) {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cortex_discarded_series_per_labelset",
			Help: "The number of series that include discarded samples for each labelset.",
		},
		[]string{"reason", "user", "labelset"},
	)

	tracker := NewDiscardedSeriesTracker(gauge)
	reason1 := "sample_out_of_bounds"
	reason2 := "label_2"
	reason3 := "unused_label"
	user1 := "user1"
	user2 := "user2"
	series1 := labels.FromStrings("__name__", "1")
	series2 := labels.FromStrings("__name__", "2")

	tracker.Track(reason1, user1, &series1)
	tracker.Track(reason2, user1, &series1)

	tracker.Track(reason1, user2, &series1)
	tracker.Track(reason1, user2, &series1)
	tracker.Track(reason1, user2, &series1)
	tracker.Track(reason1, user2, &series2)

	require.Equal(t, tracker.getSeriesCount(reason1, user1), 1)
	require.Equal(t, tracker.getSeriesCount(reason2, user1), 1)
	require.Equal(t, tracker.getSeriesCount(reason3, user1), -1)

	compareSeriesVendedCount(t, gauge, reason1, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason1, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series2, 0)

	require.Equal(t, tracker.getSeriesCount(reason1, user2), 2)
	require.Equal(t, tracker.getSeriesCount(reason2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(reason3, user2), -1)

	compareSeriesVendedCount(t, gauge, reason1, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason1, user2, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series2, 0)

	tracker.UpdateMetrics()

	tracker.Track(reason1, user1, &series1)
	tracker.Track(reason1, user1, &series1)

	require.Equal(t, tracker.getSeriesCount(reason1, user1), 1)
	require.Equal(t, tracker.getSeriesCount(reason2, user1), 0)
	require.Equal(t, tracker.getSeriesCount(reason3, user1), -1)

	compareSeriesVendedCount(t, gauge, reason1, user1, &series1, 1)
	compareSeriesVendedCount(t, gauge, reason1, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series1, 1)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series2, 0)

	require.Equal(t, tracker.getSeriesCount(reason1, user2), 0)
	require.Equal(t, tracker.getSeriesCount(reason2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(reason3, user2), -1)

	compareSeriesVendedCount(t, gauge, reason1, user2, &series1, 1)
	compareSeriesVendedCount(t, gauge, reason1, user2, &series2, 1)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series2, 0)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(reason1, user1), 0)
	require.Equal(t, tracker.getSeriesCount(reason2, user1), -1)
	require.Equal(t, tracker.getSeriesCount(reason3, user1), -1)

	compareSeriesVendedCount(t, gauge, reason1, user1, &series1, 1)
	compareSeriesVendedCount(t, gauge, reason1, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series2, 0)

	require.Equal(t, tracker.getSeriesCount(reason1, user2), -1)
	require.Equal(t, tracker.getSeriesCount(reason2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(reason3, user2), -1)

	compareSeriesVendedCount(t, gauge, reason1, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason1, user2, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series2, 0)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(reason1, user1), -1)
	require.Equal(t, tracker.getSeriesCount(reason2, user1), -1)
	require.Equal(t, tracker.getSeriesCount(reason3, user1), -1)

	compareSeriesVendedCount(t, gauge, reason1, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason1, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason2, user1, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user1, &series2, 0)

	require.Equal(t, tracker.getSeriesCount(reason1, user2), -1)
	require.Equal(t, tracker.getSeriesCount(reason2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(reason3, user2), -1)

	compareSeriesVendedCount(t, gauge, reason1, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason1, user2, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason2, user2, &series2, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series1, 0)
	compareSeriesVendedCount(t, gauge, reason3, user2, &series2, 0)
}

func compareSeriesVendedCount(t *testing.T, gaugeVec *prometheus.GaugeVec, reason string, user string, labels *labels.Labels, val int) {
	gauge, _ := gaugeVec.GetMetricWithLabelValues(reason, user, labels.String())
	require.Equal(t, testutil.ToFloat64(gauge), float64(val))
}
