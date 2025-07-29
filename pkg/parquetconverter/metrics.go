package parquetconverter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	convertedBlocks          *prometheus.CounterVec
	convertBlockFailures     *prometheus.CounterVec
	convertBlockDuration     *prometheus.GaugeVec
	convertParquetBlockDelay prometheus.Histogram
	ownedUsers               prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		convertedBlocks: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_converter_blocks_converted_total",
			Help: "Total number of blocks converted to parquet per user.",
		}, []string{"user"}),
		convertBlockFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_converter_block_convert_failures_total",
			Help: "Total number of failed block conversions per user.",
		}, []string{"user"}),
		convertBlockDuration: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_parquet_converter_convert_block_duration_seconds",
			Help: "Time taken to for the latest block conversion for the user.",
		}, []string{"user"}),
		convertParquetBlockDelay: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_parquet_converter_convert_block_delay_minutes",
			Help:    "Delay in minutes of Parquet block to be converted from the TSDB block being uploaded to object store",
			Buckets: []float64{5, 10, 15, 20, 30, 45, 60, 80, 100, 120, 150, 180, 210, 240, 270, 300},
		}),
		ownedUsers: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_parquet_converter_users_owned",
			Help: "Number of users that the parquet converter owns.",
		}),
	}
}

func (m *metrics) deleteMetricsForTenant(userID string) {
	m.convertedBlocks.DeleteLabelValues(userID)
	m.convertBlockFailures.DeleteLabelValues(userID)
	m.convertBlockDuration.DeleteLabelValues(userID)
}
