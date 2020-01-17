package ingester

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestShipperMetrics(t *testing.T) {
	mainReg := prometheus.NewRegistry()

	shipper := newShipperMetrics(mainReg)

	populateShipperMetrics(shipper.newRegistryForUser("user1"), 12345)
	populateShipperMetrics(shipper.newRegistryForUser("user2"), 85787)
	populateShipperMetrics(shipper.newRegistryForUser("user3"), 999)

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
	`), "cortex_ingester_shipper_dir_syncs_total", "cortex_ingester_shipper_dir_sync_failures_total", "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_upload_failures_total")
	require.NoError(t, err)
}

func populateShipperMetrics(r prometheus.Registerer, base float64) {
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

	r.MustRegister(dirSyncs)
	r.MustRegister(dirSyncFailures)
	r.MustRegister(uploads)
	r.MustRegister(uploadFailures)
}
