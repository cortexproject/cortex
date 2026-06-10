package queryeviction

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util/resource"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// QueryEvictor monitors system-wide resource utilization and evicts
// the heaviest running query when thresholds are breached.
type QueryEvictor struct {
	services.Service

	monitor  resource.IMonitor
	registry *QueryRegistry
	cfg      configs.EvictionConfig
	logger   log.Logger

	// Prometheus metrics
	evictionsTotal *prometheus.CounterVec // labels: resource, component
}

// NewQueryEvictor creates a new evictor. Returns nil if config is disabled.
func NewQueryEvictor(
	monitor resource.IMonitor,
	registry *QueryRegistry,
	cfg configs.EvictionConfig,
	logger log.Logger,
	reg prometheus.Registerer,
	component string,
) *QueryEvictor {
	if !cfg.Enabled() {
		return nil
	}

	e := &QueryEvictor{
		monitor:  monitor,
		registry: registry,
		cfg:      cfg,
		logger:   logger,
		evictionsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_query_evictions_total",
			Help:        "Total number of queries evicted due to resource pressure.",
			ConstLabels: map[string]string{"component": component},
		}, []string{"resource"}),
	}

	e.Service = services.NewBasicService(nil, e.running, nil)
	return e
}

// running is the main loop (called by services.Service).
func (e *QueryEvictor) running(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.CheckInterval)
	defer ticker.Stop()

	cooldownRemaining := 0

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			// If in cooldown, decrement and skip this tick.
			if cooldownRemaining > 0 {
				cooldownRemaining--
				continue
			}

			// Check system-wide resource utilization.
			breachedResource, utilization, threshold := e.checkThresholds()
			if breachedResource == "" {
				continue // no breach
			}

			// Find the heaviest running queries (up to MaxEvictionsPerCycle).
			victims := e.registry.FindHeaviest(e.cfg.MaxEvictionsPerCycle, e.cfg.MinQueryAge)
			if len(victims) == 0 {
				level.Debug(e.logger).Log(
					"msg", "resource threshold breached but no evictable queries found",
					"resource", breachedResource,
					"utilization", utilization,
					"threshold", threshold,
					"registered_queries", e.registry.Len(),
					"min_query_age", e.cfg.MinQueryAge,
				)
				continue
			}

			// Evict each victim.
			for _, victim := range victims {
				metricValue := e.registry.metric(victim.Stats)
				victim.Cancel()

				level.Warn(e.logger).Log(
					"msg", "evicting query due to resource pressure",
					"resource", breachedResource,
					"utilization", utilization,
					"threshold", threshold,
					"request_id", victim.RequestID,
					"query", victim.QueryExpr,
					"user", victim.UserID,
					"metric", e.cfg.EvictionMetric,
					"metric_value", metricValue,
				)

				e.evictionsTotal.WithLabelValues(string(breachedResource)).Inc()
			}

			// Enter cooldown.
			cooldownRemaining = e.cfg.CooldownPeriod
		}
	}
}

// checkThresholds returns the first breached resource type, its current
// utilization, and the configured threshold. Returns ("", 0, 0) if no breach.
// CPU is checked before heap (deterministic priority).
func (e *QueryEvictor) checkThresholds() (resource.Type, float64, float64) {
	if e.cfg.Threshold.CPUUtilization > 0 {
		cpuUtil := e.monitor.GetCPUUtilization()
		if cpuUtil >= e.cfg.Threshold.CPUUtilization {
			return resource.CPU, cpuUtil, e.cfg.Threshold.CPUUtilization
		}
	}

	if e.cfg.Threshold.HeapUtilization > 0 {
		heapUtil := e.monitor.GetHeapUtilization()
		if heapUtil >= e.cfg.Threshold.HeapUtilization {
			return resource.Heap, heapUtil, e.cfg.Threshold.HeapUtilization
		}
	}

	return "", 0, 0
}
