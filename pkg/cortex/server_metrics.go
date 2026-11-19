package cortex

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
)

// newServerMetrics returns the HTTP and gRPC server metrics. It starts from the
// metrics of the weaveworks/common server, and replaces the request and response
// message size histograms with histograms that also expose native buckets. The
// server library sets native buckets only on the request duration histogram, and
// gives no option to set them on the other histograms.
//
// The native bucket factor comes from cfg.MetricsNativeHistogramFactor, the same
// factor that the server library uses for the request duration histogram. When the
// factor is 0, the histograms expose classic buckets only.
func newServerMetrics(cfg server.Config) *server.Metrics {
	metrics := server.NewServerMetrics(cfg)
	metrics.ReceivedMessageSize = newMessageSizeHistogram(cfg,
		"request_message_bytes",
		"Size (in bytes) of messages received in the request.")
	metrics.SentMessageSize = newMessageSizeHistogram(cfg,
		"response_message_bytes",
		"Size (in bytes) of messages sent in response.")
	return metrics
}

func newMessageSizeHistogram(cfg server.Config, name, help string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       cfg.MetricsNamespace,
		Name:                            name,
		Help:                            help,
		Buckets:                         middleware.BodySizeBuckets,
		NativeHistogramBucketFactor:     cfg.MetricsNativeHistogramFactor,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"method", "route"})
}

// newServer makes the HTTP and gRPC server with the metrics from newServerMetrics.
// It registers the metrics like server.New does: with cfg.Registerer when it is
// set, and with the Prometheus default registerer when it is not set.
func newServer(cfg server.Config) (*server.Server, error) {
	registerer := cfg.Registerer
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	metrics := newServerMetrics(cfg)
	metrics.MustRegister(registerer)

	return server.NewWithMetrics(cfg, metrics)
}
