package ingester

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTSDBMetrics(t *testing.T) {
	mainReg := prometheus.NewRegistry()

	tsdbMetrics := newTSDBMetrics(mainReg)

	tsdbMetrics.setRegistryForUser("user1", populateTSDBMetrics(12345))
	tsdbMetrics.setRegistryForUser("user2", populateTSDBMetrics(85787))
	tsdbMetrics.setRegistryForUser("user3", populateTSDBMetrics(999))

	metricNames := []string{
		"cortex_ingester_shipper_dir_syncs_total",
		"cortex_ingester_shipper_dir_sync_failures_total",
		"cortex_ingester_shipper_uploads_total",
		"cortex_ingester_shipper_upload_failures_total",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
	}

	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_ingester_shipper_dir_syncs_total TSDB: Total dir sync attempts
			# TYPE cortex_ingester_shipper_dir_syncs_total counter
			# 12345 + 85787 + 999
			cortex_ingester_shipper_dir_syncs_total 99131

			# HELP cortex_ingester_shipper_dir_sync_failures_total TSDB: Total number of failed dir syncs
			# TYPE cortex_ingester_shipper_dir_sync_failures_total counter
			# 2*(12345 + 85787 + 999)
			cortex_ingester_shipper_dir_sync_failures_total 198262

			# HELP cortex_ingester_shipper_uploads_total TSDB: Total object upload attempts
			# TYPE cortex_ingester_shipper_uploads_total counter
			# 3*(12345 + 85787 + 999)
			cortex_ingester_shipper_uploads_total 297393

			# HELP cortex_ingester_shipper_upload_failures_total TSDB: Total number of failed object uploads
			# TYPE cortex_ingester_shipper_upload_failures_total counter
			# 4*(12345 + 85787 + 999)
			cortex_ingester_shipper_upload_failures_total 396524

			# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
			# TYPE cortex_ingester_memory_series_created_total counter
			# 5 * (12345, 85787 and 999 respectively)
			cortex_ingester_memory_series_created_total{user="user1"} 61725
			cortex_ingester_memory_series_created_total{user="user2"} 428935
			cortex_ingester_memory_series_created_total{user="user3"} 4995

			# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
			# TYPE cortex_ingester_memory_series_removed_total counter
			# 6 * (12345, 85787 and 999 respectively)
			cortex_ingester_memory_series_removed_total{user="user1"} 74070
			cortex_ingester_memory_series_removed_total{user="user2"} 514722
			cortex_ingester_memory_series_removed_total{user="user3"} 5994
	`), metricNames...)
	require.NoError(t, err)
}

func populateTSDBMetrics(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()

	// shipper
	dirSyncs := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_syncs_total",
		Help: "Total dir sync attempts",
	})
	dirSyncs.Add(1 * base)

	dirSyncFailures := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_sync_failures_total",
		Help: "Total number of failed dir syncs",
	})
	dirSyncFailures.Add(2 * base)

	uploads := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_uploads_total",
		Help: "Total object upload attempts",
	})
	uploads.Add(3 * base)

	uploadFailures := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_upload_failures_total",
		Help: "Total number of failed object uploads",
	})
	uploadFailures.Add(4 * base)

	// TSDB Head
	seriesCreated := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_created_total",
	})
	seriesCreated.Add(5 * base)

	seriesRemoved := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_removed_total",
	})
	seriesRemoved.Add(6 * base)

	r.MustRegister(dirSyncs)
	r.MustRegister(dirSyncFailures)
	r.MustRegister(uploads)
	r.MustRegister(uploadFailures)
	r.MustRegister(seriesCreated)
	r.MustRegister(seriesRemoved)

	return r
}
