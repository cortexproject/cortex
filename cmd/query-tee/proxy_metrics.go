package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
)

type ProxyMetrics struct {
	durationMetric *prometheus.HistogramVec
}

func NewProxyMetrics(registerer prometheus.Registerer) *ProxyMetrics {
	m := &ProxyMetrics{
		durationMetric: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex_querytee",
			Name:      "request_duration_seconds",
			Help:      "Time (in seconds) spent serving HTTP requests.",
			Buckets:   instrument.DefBuckets,
		}, []string{"backend", "method", "route", "status_code"}),
	}

	if registerer != nil {
		registerer.MustRegister(m.durationMetric)
	}

	return m
}
