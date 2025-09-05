package cortexotlpconverter

import (
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func otelMetricTypeToPromMetricType(otelMetric pmetric.Metric) cortexpb.MetricMetadata_MetricType {
	switch otelMetric.Type() {
	case pmetric.MetricTypeGauge:
		return cortexpb.GAUGE
	case pmetric.MetricTypeSum:
		metricType := cortexpb.GAUGE
		if otelMetric.Sum().IsMonotonic() {
			metricType = cortexpb.COUNTER
		}
		// We're in an early phase of implementing delta support (proposal: https://github.com/prometheus/proposals/pull/48/)
		// We don't have a proper way to flag delta metrics yet, therefore marking the metric type as unknown for now.
		if otelMetric.Sum().AggregationTemporality() == pmetric.AggregationTemporalityDelta {
			metricType = cortexpb.UNKNOWN
		}
		return metricType
	case pmetric.MetricTypeHistogram:
		// We're in an early phase of implementing delta support (proposal: https://github.com/prometheus/proposals/pull/48/)
		// We don't have a proper way to flag delta metrics yet, therefore marking the metric type as unknown for now.
		if otelMetric.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityDelta {
			return cortexpb.UNKNOWN
		}
		return cortexpb.HISTOGRAM
	case pmetric.MetricTypeSummary:
		return cortexpb.SUMMARY
	case pmetric.MetricTypeExponentialHistogram:
		if otelMetric.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityDelta {
			// We're in an early phase of implementing delta support (proposal: https://github.com/prometheus/proposals/pull/48/)
			// We don't have a proper way to flag delta metrics yet, therefore marking the metric type as unknown for now.
			return cortexpb.UNKNOWN
		}
		return cortexpb.HISTOGRAM
	}
	return cortexpb.UNKNOWN
}
