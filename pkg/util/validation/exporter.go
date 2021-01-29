package validation

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util/services"
)

const typeTenant = "tenant"

// OverridesExporter exposes per-tenant resource limit overrides as Prometheus metrics
type OverridesExporter struct {
	services.Service

	overridesCh  <-chan map[string]*Limits
	overridesMtx sync.RWMutex
	overrides    map[string]*Limits

	description *prometheus.Desc
}

// NewOverridesExporter creates an OverridesExporter that reads updates to per-tenant
// limits from the provided channel. A single instance of the tenant to limit mapping
// is kept in memory at a time by this exporter.
func NewOverridesExporter(overridesCh <-chan map[string]*Limits) *OverridesExporter {
	exporter := &OverridesExporter{
		overridesCh:  overridesCh,
		overridesMtx: sync.RWMutex{},

		// The name and labels for this metric are picked for compatibility with an existing tool
		// that serves the same purpose (though it lives outside of Cortex):
		// https://github.com/grafana/cortex-tools/blob/98415629764544b96c7725a0282ff4240db5eb54/pkg/commands/overrides_exporter.go
		description: prometheus.NewDesc(
			"cortex_overrides",
			"Resource limit overrides applied to tenants",
			[]string{"limit_type", "type", "user"},
			nil,
		),
	}

	exporter.Service = services.NewBasicService(nil, exporter.loop, nil)
	return exporter
}

func (oe *OverridesExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- oe.description
}

func (oe *OverridesExporter) Collect(ch chan<- prometheus.Metric) {
	oe.overridesMtx.RLock()
	defer oe.overridesMtx.RUnlock()

	if oe.overrides == nil {
		return
	}

	for tenant, limits := range oe.overrides {
		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, limits.IngestionRate, "ingestion_rate", typeTenant, tenant)
		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, float64(limits.IngestionBurstSize), "ingestion_burst_size", typeTenant, tenant)

		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, float64(limits.MaxSeriesPerQuery), "max_series_per_query", typeTenant, tenant)
		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, float64(limits.MaxSamplesPerQuery), "max_samples_per_query", typeTenant, tenant)
		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, float64(limits.MaxLocalSeriesPerUser), "max_local_series_per_user", typeTenant, tenant)
		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, float64(limits.MaxLocalSeriesPerMetric), "max_local_series_per_metric", typeTenant, tenant)
		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerUser), "max_global_series_per_user", typeTenant, tenant)
		ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerMetric), "max_global_series_per_metric", typeTenant, tenant)
	}
}

// loop runs until cancelled, storing updates to per-tenant limit overrides
func (oe *OverridesExporter) loop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case cfg := <-oe.overridesCh:
			oe.overridesMtx.Lock()
			oe.overrides = cfg
			oe.overridesMtx.Unlock()
		}
	}
}
