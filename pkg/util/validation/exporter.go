package validation

import (
	"reflect"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// OverridesExporter exposes per-tenant resource limit overrides as Prometheus metrics
type OverridesExporter struct {
	tenantLimits TenantLimits
	description  *prometheus.Desc
}

// NewOverridesExporter creates an OverridesExporter that reads updates to per-tenant
// limits using the provided function.
func NewOverridesExporter(tenantLimits TenantLimits) *OverridesExporter {
	return &OverridesExporter{
		tenantLimits: tenantLimits,
		description: prometheus.NewDesc(
			"cortex_overrides",
			"Resource limit overrides applied to tenants",
			[]string{"limit_name", "user"},
			nil,
		),
	}
}

func (oe *OverridesExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- oe.description
}

func (oe *OverridesExporter) Collect(ch chan<- prometheus.Metric) {
	allLimits := oe.tenantLimits.AllByUserID()
	for tenant, limits := range allLimits {
		for metricName, value := range ExtractNumericalValues(limits) {
			ch <- prometheus.MustNewConstMetric(oe.description, prometheus.GaugeValue, value, metricName, tenant)
		}
	}
}

func ExtractNumericalValues(l *Limits) map[string]float64 {
	metrics := make(map[string]float64)

	v := reflect.ValueOf(l).Elem()
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		tag := fieldType.Tag.Get("yaml")
		if tag == "" || tag == "-" {
			// not exist tag or tag is "-"
			continue
		}

		// remove options like omitempty
		if idx := strings.Index(tag, ","); idx != -1 {
			tag = tag[:idx]
		}

		switch field.Kind() {
		case reflect.Int, reflect.Int64:
			switch fieldType.Type.String() {
			case "model.Duration":
				// we export the model.Duration in seconds
				metrics[tag] = time.Duration(field.Int()).Seconds()
			case "model.ValidationScheme":
				// skip
			default:
				metrics[tag] = float64(field.Int())
			}
		case reflect.Uint, reflect.Uint64:
			metrics[tag] = float64(field.Uint())
		case reflect.Float64:
			metrics[tag] = field.Float()
		case reflect.Bool:
			if field.Bool() {
				// true as 1.0
				metrics[tag] = 1.0
			} else {
				// false as 0.0
				metrics[tag] = 0.0
			}
		case reflect.String, reflect.Slice, reflect.Map, reflect.Struct:
			continue
		}
	}
	return metrics
}
