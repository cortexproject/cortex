package cortexpb

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

func (e *ExemplarV2) ToLabels(b *labels.ScratchBuilder, symbols []string) (labels.Labels, error) {
	return desymbolizeLabels(b, e.GetLabelsRefs(), symbols)
}

func (t *TimeSeriesV2) ToLabels(b *labels.ScratchBuilder, symbols []string) (labels.Labels, error) {
	return desymbolizeLabels(b, t.GetLabelsRefs(), symbols)
}

// desymbolizeLabels decodes label references, with given symbols to labels.
// Copied from the Prometheus: https://github.com/prometheus/prometheus/blob/v3.7.2/prompb/io/prometheus/write/v2/symbols.go#L83
func desymbolizeLabels(b *labels.ScratchBuilder, labelRefs []uint32, symbols []string) (labels.Labels, error) {
	if len(labelRefs)%2 != 0 {
		return labels.EmptyLabels(), fmt.Errorf("invalid labelRefs length %d", len(labelRefs))
	}

	b.Reset()
	for i := 0; i < len(labelRefs); i += 2 {
		nameRef, valueRef := labelRefs[i], labelRefs[i+1]
		if int(nameRef) >= len(symbols) || int(valueRef) >= len(symbols) {
			return labels.EmptyLabels(), fmt.Errorf("labelRefs %d (name) = %d (value) outside of symbols table (size %d)", nameRef, valueRef, len(symbols))
		}
		b.Add(symbols[nameRef], symbols[valueRef])
	}
	b.Sort()
	return b.Labels(), nil
}

func MetadataV2MetricTypeToMetricType(mt MetadataV2_MetricType) model.MetricType {
	switch mt {
	case METRIC_TYPE_UNSPECIFIED:
		return model.MetricTypeUnknown
	case METRIC_TYPE_COUNTER:
		return model.MetricTypeCounter
	case METRIC_TYPE_GAUGE:
		return model.MetricTypeGauge
	case METRIC_TYPE_HISTOGRAM:
		return model.MetricTypeHistogram
	case METRIC_TYPE_GAUGEHISTOGRAM:
		return model.MetricTypeGaugeHistogram
	case METRIC_TYPE_SUMMARY:
		return model.MetricTypeSummary
	case METRIC_TYPE_INFO:
		return model.MetricTypeInfo
	case METRIC_TYPE_STATESET:
		return model.MetricTypeStateset
	default:
		return model.MetricTypeUnknown
	}
}
