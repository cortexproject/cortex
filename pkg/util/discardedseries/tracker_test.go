package discardedseries

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestLabelSetTracker(t *testing.T) {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cortex_discarded_series_total",
			Help: "The total number of series that include discarded samples.",
		},
		[]string{"reason", "user", "series"},
	)
	tracker := NewDiscardedSeriesTracker(gauge)
	label1 := "sample_out_of_bounds"
	label2 := "new_value_for_timestamp"
	label3 := "invalid_label_name"
	user1 := "user1"
	user2 := "user2"
	series1 := "series1"
	series2 := "series2"

	tracker.Track(label1, user1, series1)
	tracker.Track(label2, user1, series1)
	tracker.Track(label3, user1, series1)

	tracker.Track(label1, user2, series1)
	tracker.Track(label1, user2, series1)
	tracker.Track(label1, user2, series2)

	require.Equal(t, tracker.getSeriesCount(label1, user1, series1), 1)
	require.Equal(t, tracker.getSeriesCount(label2, user1, series1), 1)
	require.Equal(t, tracker.getSeriesCount(label3, user1, series1), -1)
	require.Equal(t, tracker.getSeriesCount(label1, user1, series2), -1)

	require.Equal(t, tracker.getSeriesCount(label1, user2, series1), 2)
	require.Equal(t, tracker.getSeriesCount(label2, user2, series2), 1)

	_, err := gauge.GetMetricWithLabelValues(label1, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label2, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label3, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label1, user1, series2)
	require.Error(t, err)

	_, err = gauge.GetMetricWithLabelValues(label1, user2, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label2, user2, series2)
	require.Error(t, err)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(label1, user1, series1), 0)
	require.Equal(t, tracker.getSeriesCount(label2, user1, series1), 0)
	require.Equal(t, tracker.getSeriesCount(label3, user1, series1), -1)
	require.Equal(t, tracker.getSeriesCount(label1, user1, series2), -1)

	require.Equal(t, tracker.getSeriesCount(label1, user2, series1), 0)
	require.Equal(t, tracker.getSeriesCount(label2, user2, series2), 0)

	metric, err := gauge.GetMetricWithLabelValues(label1, user1, series1)
	require.NoError(t, err)
	require.Equal(t, metric, 1)
	_, err = gauge.GetMetricWithLabelValues(label2, user1, series1)
	require.NoError(t, err)
	require.Equal(t, metric, 1)
	_, err = gauge.GetMetricWithLabelValues(label3, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label1, user1, series2)
	require.Error(t, err)

	_, err = gauge.GetMetricWithLabelValues(label1, user2, series1)
	require.NoError(t, err)
	require.Equal(t, metric, 2)
	_, err = gauge.GetMetricWithLabelValues(label2, user2, series2)
	require.NoError(t, err)
	require.Equal(t, metric, 1)

	tracker.UpdateMetrics()

	require.Equal(t, tracker.getSeriesCount(label1, user1, series1), -1)
	require.Equal(t, tracker.getSeriesCount(label2, user1, series1), -1)
	require.Equal(t, tracker.getSeriesCount(label3, user1, series1), -1)
	require.Equal(t, tracker.getSeriesCount(label1, user1, series2), -1)

	require.Equal(t, tracker.getSeriesCount(label1, user2, series1), -1)
	require.Equal(t, tracker.getSeriesCount(label2, user2, series2), -1)

	metric, err = gauge.GetMetricWithLabelValues(label1, user1, series1)
	require.NoError(t, err)
	require.Equal(t, metric, 0)
	_, err = gauge.GetMetricWithLabelValues(label2, user1, series1)
	require.NoError(t, err)
	require.Equal(t, metric, 0)
	_, err = gauge.GetMetricWithLabelValues(label3, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label1, user1, series2)
	require.Error(t, err)

	_, err = gauge.GetMetricWithLabelValues(label1, user2, series1)
	require.NoError(t, err)
	require.Equal(t, metric, 0)
	_, err = gauge.GetMetricWithLabelValues(label2, user2, series2)
	require.NoError(t, err)
	require.Equal(t, metric, 0)

	tracker.UpdateMetrics()

	_, err = gauge.GetMetricWithLabelValues(label1, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label2, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label3, user1, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label1, user1, series2)
	require.Error(t, err)

	_, err = gauge.GetMetricWithLabelValues(label1, user2, series1)
	require.Error(t, err)
	_, err = gauge.GetMetricWithLabelValues(label2, user2, series2)
	require.Error(t, err)

}
