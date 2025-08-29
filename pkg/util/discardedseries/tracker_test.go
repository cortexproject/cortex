package discardedseries

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestLabelSetTracker(t *testing.T) {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cortex_discarded_series_total",
			Help: "The total number of series that include discarded samples.",
		},
		[]string{"reason", "user"},
	)

	tracker := NewDiscardedSeriesTracker(gauge)
	label1 := "sample_out_of_bounds"
	label2 := "label_2"
	label3 := "unused_label"
	user1 := "user1"
	user2 := "user2"
	series1 := uint64(1)
	series2 := uint64(2)

	tracker.Track(label1, user1, series1)
	tracker.Track(label2, user1, series1)

	tracker.Track(label1, user2, series1)
	tracker.Track(label1, user2, series1)
	tracker.Track(label1, user2, series1)
	tracker.Track(label1, user2, series2)

	require.Equal(t, tracker.getSeriesCount(label1, user1), 1)
	require.Equal(t, tracker.getSeriesCount(label2, user1), 1)
	require.Equal(t, tracker.getSeriesCount(label3, user1), -1)

	compareSeriesVendedCount(t, gauge, label1, user1, 0)
	compareSeriesVendedCount(t, gauge, label2, user1, 0)
	compareSeriesVendedCount(t, gauge, label3, user1, 0)

	require.Equal(t, tracker.getSeriesCount(label1, user2), 2)
	require.Equal(t, tracker.getSeriesCount(label2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(label3, user2), -1)

	compareSeriesVendedCount(t, gauge, label1, user2, 0)
	compareSeriesVendedCount(t, gauge, label2, user2, 0)
	compareSeriesVendedCount(t, gauge, label3, user2, 0)

	tracker.UpdateMetrics()

	tracker.Track(label1, user1, series1)
	tracker.Track(label1, user1, series1)

	require.Equal(t, tracker.getSeriesCount(label1, user1), 1)
	require.Equal(t, tracker.getSeriesCount(label2, user1), 0)
	require.Equal(t, tracker.getSeriesCount(label3, user1), -1)

	compareSeriesVendedCount(t, gauge, label1, user1, 1)
	compareSeriesVendedCount(t, gauge, label2, user1, 1)
	compareSeriesVendedCount(t, gauge, label3, user1, 0)

	require.Equal(t, tracker.getSeriesCount(label1, user2), 0)
	require.Equal(t, tracker.getSeriesCount(label2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(label3, user2), -1)

	compareSeriesVendedCount(t, gauge, label1, user2, 2)
	compareSeriesVendedCount(t, gauge, label2, user2, 0)
	compareSeriesVendedCount(t, gauge, label3, user2, 0)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(label1, user1), 0)
	require.Equal(t, tracker.getSeriesCount(label2, user1), -1)
	require.Equal(t, tracker.getSeriesCount(label3, user1), -1)

	compareSeriesVendedCount(t, gauge, label1, user1, 1)
	compareSeriesVendedCount(t, gauge, label2, user1, 0)
	compareSeriesVendedCount(t, gauge, label3, user1, 0)

	require.Equal(t, tracker.getSeriesCount(label1, user2), -1)
	require.Equal(t, tracker.getSeriesCount(label2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(label3, user2), -1)

	compareSeriesVendedCount(t, gauge, label1, user2, 0)
	compareSeriesVendedCount(t, gauge, label2, user2, 0)
	compareSeriesVendedCount(t, gauge, label3, user2, 0)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(label1, user1), -1)
	require.Equal(t, tracker.getSeriesCount(label2, user1), -1)
	require.Equal(t, tracker.getSeriesCount(label3, user1), -1)

	compareSeriesVendedCount(t, gauge, label1, user1, 0)
	compareSeriesVendedCount(t, gauge, label2, user1, 0)
	compareSeriesVendedCount(t, gauge, label3, user1, 0)

	require.Equal(t, tracker.getSeriesCount(label1, user2), -1)
	require.Equal(t, tracker.getSeriesCount(label2, user2), -1)
	require.Equal(t, tracker.getSeriesCount(label3, user2), -1)

	compareSeriesVendedCount(t, gauge, label1, user2, 0)
	compareSeriesVendedCount(t, gauge, label2, user2, 0)
	compareSeriesVendedCount(t, gauge, label3, user2, 0)
}

func compareSeriesVendedCount(t *testing.T, gaugeVec *prometheus.GaugeVec, label string, user string, val int) {
	gauge, _ := gaugeVec.GetMetricWithLabelValues(label, user)
	require.Equal(t, testutil.ToFloat64(gauge), float64(val))
}
