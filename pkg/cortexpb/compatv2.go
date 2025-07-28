package cortexpb

import "github.com/prometheus/prometheus/model/labels"

func (e *ExemplarV2) ToLabels(b *labels.ScratchBuilder, symbols []string) labels.Labels {
	return desymbolizeLabels(b, e.GetLabelsRefs(), symbols)
}

func (t *TimeSeriesV2) ToLabels(b *labels.ScratchBuilder, symbols []string) labels.Labels {
	return desymbolizeLabels(b, t.GetLabelsRefs(), symbols)
}

// desymbolizeLabels decodes label references, with given symbols to labels.
// Copied from the Prometheus: https://github.com/prometheus/prometheus/blob/v3.5.0/prompb/io/prometheus/write/v2/symbols.go#L76
func desymbolizeLabels(b *labels.ScratchBuilder, labelRefs []uint32, symbols []string) labels.Labels {
	b.Reset()
	for i := 0; i < len(labelRefs); i += 2 {
		b.Add(symbols[labelRefs[i]], symbols[labelRefs[i+1]])
	}
	b.Sort()
	return b.Labels()
}
