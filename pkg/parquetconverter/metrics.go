package parquetconverter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	convertedBlocks      *prometheus.CounterVec
	convertBlockDuration *prometheus.GaugeVec
	ownedUsers           prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		convertedBlocks: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_converter_converted_blocks_total",
			Help: "Total number of converted blocks per user.",
		}, []string{"user"}),
		convertBlockDuration: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_parquet_converter_convert_block_duration_seconds",
			Help: "Time taken to for the latest block conversion for the user.",
		}, []string{"user"}),
		ownedUsers: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_parquet_converter_users_owned",
			Help: "Number of users that the parquet converter owns.",
		}),
	}
}

func (m *metrics) deleteMetricsForTenant(userID string) {
	m.convertedBlocks.DeleteLabelValues(userID)
	m.convertBlockDuration.DeleteLabelValues(userID)
}
