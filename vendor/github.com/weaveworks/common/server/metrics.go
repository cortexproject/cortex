package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/middleware"
	"time"
)

type Metrics struct {
	TcpConnections      *prometheus.GaugeVec
	TcpConnectionsLimit *prometheus.GaugeVec
	RequestDuration     *prometheus.HistogramVec
	ReceivedMessageSize *prometheus.HistogramVec
	SentMessageSize     *prometheus.HistogramVec
	InflightRequests    *prometheus.GaugeVec
}

func NewServerMetrics(cfg Config) *Metrics {
	return &Metrics{
		TcpConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "tcp_connections",
			Help:      "Current number of accepted TCP connections.",
		}, []string{"protocol"}),
		TcpConnectionsLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "tcp_connections_limit",
			Help:      "The max number of TCP connections that can be accepted (0 means no limit).",
		}, []string{"protocol"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                       cfg.MetricsNamespace,
			Name:                            "request_duration_seconds",
			Help:                            "Time (in seconds) spent serving HTTP requests.",
			Buckets:                         instrument.DefBuckets,
			NativeHistogramBucketFactor:     cfg.MetricsNativeHistogramFactor,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"method", "route", "status_code", "ws"}),
		ReceivedMessageSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "request_message_bytes",
			Help:      "Size (in bytes) of messages received in the request.",
			Buckets:   middleware.BodySizeBuckets,
		}, []string{"method", "route"}),
		SentMessageSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "response_message_bytes",
			Help:      "Size (in bytes) of messages sent in response.",
			Buckets:   middleware.BodySizeBuckets,
		}, []string{"method", "route"}),
		InflightRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "inflight_requests",
			Help:      "Current number of inflight requests.",
		}, []string{"method", "route"}),
	}
}

func (s *Metrics) MustRegister(registerer prometheus.Registerer) {
	registerer.MustRegister(
		s.TcpConnections,
		s.TcpConnectionsLimit,
		s.RequestDuration,
		s.ReceivedMessageSize,
		s.SentMessageSize,
		s.InflightRequests,
	)
}
