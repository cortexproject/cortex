package ring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type BasicLifecyclerMetrics struct {
	heartbeats  prometheus.Counter
	tokensOwned prometheus.Gauge
	tokensToOwn prometheus.Gauge
	zoneInfo    prometheus.GaugeVec
}

func NewBasicLifecyclerMetrics(ringName string, zone string, reg prometheus.Registerer) *BasicLifecyclerMetrics {
	zoneInfo := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "ring_availability_zone",
		Help: "The availability zone of the instance",
	}, []string{"az"})
	zoneInfo.WithLabelValues(zone).Set(1)

	return &BasicLifecyclerMetrics{
		heartbeats: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "ring_member_heartbeats_total",
			Help:        "The total number of heartbeats sent.",
			ConstLabels: prometheus.Labels{"name": ringName},
		}),
		tokensOwned: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name:        "ring_member_tokens_owned",
			Help:        "The number of tokens owned in the ring.",
			ConstLabels: prometheus.Labels{"name": ringName},
		}),
		tokensToOwn: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name:        "ring_member_tokens_to_own",
			Help:        "The number of tokens to own in the ring.",
			ConstLabels: prometheus.Labels{"name": ringName},
		}),
		zoneInfo: *zoneInfo,
	}
}
