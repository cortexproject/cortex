package tsdb

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

type MatcherCacheMetrics struct {
	r      *prometheus.Registry
	logger log.Logger

	requestsTotal *prometheus.Desc
	hitsTotal     *prometheus.Desc
	numItems      *prometheus.Desc
	maxItems      *prometheus.Desc
	evicted       *prometheus.Desc
}

func NewMatchCacheMetrics(prefix string, r *prometheus.Registry, l log.Logger) *MatcherCacheMetrics {
	m := &MatcherCacheMetrics{
		r:      r,
		logger: l,
		requestsTotal: prometheus.NewDesc(
			fmt.Sprintf("%v_matchers_cache_requests_total", prefix),
			"Total number of cache requests for series matchers",
			nil, nil),
		hitsTotal: prometheus.NewDesc(
			fmt.Sprintf("%v_matchers_cache_hits_total", prefix),
			"Total number of cache hits for series matchers",
			nil, nil),
		numItems: prometheus.NewDesc(
			fmt.Sprintf("%v_matchers_cache_items", prefix),
			"Total number of cached items",
			nil, nil),
		maxItems: prometheus.NewDesc(
			fmt.Sprintf("%v_matchers_cache_max_items", prefix),
			"Maximum number of items that can be cached",
			nil, nil),
		evicted: prometheus.NewDesc(
			fmt.Sprintf("%v_matchers_cache_evicted_total", prefix),
			"Total number of items evicted from the cache",
			nil, nil),
	}
	return m
}

func (m *MatcherCacheMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- m.requestsTotal
	out <- m.hitsTotal
	out <- m.numItems
	out <- m.maxItems
	out <- m.evicted
}

func (m *MatcherCacheMetrics) Collect(out chan<- prometheus.Metric) {
	gm, err := m.r.Gather()
	if err != nil {
		level.Warn(m.logger).Log("msg", "failed to gather metrics from registry", "err", err)
		return
	}

	mfm, err := util.NewMetricFamilyMap(gm)

	if err != nil {
		level.Warn(m.logger).Log("msg", "failed to create metric family map", "err", err)
		return
	}

	out <- prometheus.MustNewConstMetric(m.requestsTotal, prometheus.CounterValue, mfm.SumCounters("thanos_matchers_cache_requests_total"))
	out <- prometheus.MustNewConstMetric(m.hitsTotal, prometheus.CounterValue, mfm.SumCounters("thanos_matchers_cache_hits_total"))
	out <- prometheus.MustNewConstMetric(m.numItems, prometheus.GaugeValue, mfm.SumGauges("thanos_matchers_cache_items"))
	out <- prometheus.MustNewConstMetric(m.maxItems, prometheus.GaugeValue, mfm.SumGauges("thanos_matchers_cache_max_items"))
	out <- prometheus.MustNewConstMetric(m.evicted, prometheus.CounterValue, mfm.SumCounters("thanos_matchers_cache_evicted_total"))
}
