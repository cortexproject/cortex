package ingester

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTSDBMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	tsdbMetrics := newTSDBMetrics(mainReg)

	tsdbMetrics.setRegistryForUser("user1", populateTSDBMetrics(12345))
	tsdbMetrics.setRegistryForUser("user2", populateTSDBMetrics(85787))
	tsdbMetrics.setRegistryForUser("user3", populateTSDBMetrics(999))

	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_ingester_shipper_dir_syncs_total TSDB: Total number of dir syncs
			# TYPE cortex_ingester_shipper_dir_syncs_total counter
			# 12345 + 85787 + 999
			cortex_ingester_shipper_dir_syncs_total 99131

			# HELP cortex_ingester_shipper_dir_sync_failures_total TSDB: Total number of failed dir syncs
			# TYPE cortex_ingester_shipper_dir_sync_failures_total counter
			# 2*(12345 + 85787 + 999)
			cortex_ingester_shipper_dir_sync_failures_total 198262

			# HELP cortex_ingester_shipper_uploads_total TSDB: Total number of uploaded blocks
			# TYPE cortex_ingester_shipper_uploads_total counter
			# 3*(12345 + 85787 + 999)
			cortex_ingester_shipper_uploads_total 297393

			# HELP cortex_ingester_shipper_upload_failures_total TSDB: Total number of block upload failures
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
	`))
	require.NoError(t, err)
}

func populateTSDBMetrics(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()

	// shipper
	dirSyncs := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_syncs_total",
		Help: "Total number of dir syncs",
	})
	dirSyncs.Add(1 * base)

	dirSyncFailures := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_sync_failures_total",
		Help: "Total number of failed dir syncs",
	})
	dirSyncFailures.Add(2 * base)

	uploads := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_uploads_total",
		Help: "Total number of uploaded blocks",
	})
	uploads.Add(3 * base)

	uploadFailures := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_upload_failures_total",
		Help: "Total number of block upload failures",
	})
	uploadFailures.Add(4 * base)

	// TSDB Head
	seriesCreated := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_created_total",
	})
	seriesCreated.Add(5 * base)

	seriesRemoved := promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_removed_total",
	})
	seriesRemoved.Add(6 * base)

	return r
}
