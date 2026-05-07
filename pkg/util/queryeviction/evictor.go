package queryeviction

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/util/resource"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// EvictionConfig configures the resource-based query evictor.
type EvictionConfig struct {
	CPUUtilization  float64       `yaml:"cpu_utilization"`
	HeapUtilization float64       `yaml:"heap_utilization"`
	CheckInterval   time.Duration `yaml:"check_interval"`
	CooldownPeriod  int           `yaml:"cooldown_period"`
	EvictionMetric  string        `yaml:"eviction_metric"`
	MinQueryAge     time.Duration `yaml:"min_query_age"`
}

// Enabled returns true if at least one threshold is > 0.
func (c EvictionConfig) Enabled() bool {
	return c.CPUUtilization > 0 || c.HeapUtilization > 0
}

// QueryEvictor monitors system-wide resource utilization and evicts
// the heaviest running query when thresholds are breached.
type QueryEvictor struct {
	services.Service

	monitor  resource.IMonitor
	registry *QueryRegistry
	cfg      EvictionConfig
	logger   log.Logger

	// Prometheus metrics
	evictionsTotal *prometheus.CounterVec // labels: resource, component
}

// NewQueryEvictor creates a new evictor. Returns nil if config is disabled.
func NewQueryEvictor(
	monitor resource.IMonitor,
	registry *QueryRegistry,
	cfg EvictionConfig,
	logger log.Logger,
	reg prometheus.Registerer,
	component string,
) (*QueryEvictor, error) {
	if !cfg.Enabled() {
		return nil, nil
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
	return e, nil
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

			// Find the heaviest running query.
			heaviest := e.registry.FindHeaviest(e.cfg.MinQueryAge)
			if heaviest == nil {
				continue // no running queries to evict
			}

			// Evict the heaviest query.
			metricValue := e.registry.metric(heaviest.Stats)
			heaviest.Cancel()

			// Log the eviction.
			level.Warn(e.logger).Log(
				"msg", "evicting heaviest query due to resource pressure",
				"resource", breachedResource,
				"utilization", utilization,
				"threshold", threshold,
				"request_id", heaviest.RequestID,
				"query", heaviest.QueryExpr,
				"user", heaviest.UserID,
				"metric", e.cfg.EvictionMetric,
				"metric_value", metricValue,
			)

			// Increment metrics.
			e.evictionsTotal.WithLabelValues(string(breachedResource)).Inc()

			// Enter cooldown.
			cooldownRemaining = e.cfg.CooldownPeriod
		}
	}
}

// checkThresholds returns the first breached resource type, its current
// utilization, and the configured threshold. Returns ("", 0, 0) if no breach.
// CPU is checked before heap (deterministic priority).
func (e *QueryEvictor) checkThresholds() (resource.Type, float64, float64) {
	if e.cfg.CPUUtilization > 0 {
		cpuUtil := e.monitor.GetCPUUtilization()
		if cpuUtil >= e.cfg.CPUUtilization {
			return resource.CPU, cpuUtil, e.cfg.CPUUtilization
		}
	}

	if e.cfg.HeapUtilization > 0 {
		heapUtil := e.monitor.GetHeapUtilization()
		if heapUtil >= e.cfg.HeapUtilization {
			return resource.Heap, heapUtil, e.cfg.HeapUtilization
		}
	}

	return "", 0, 0
}
