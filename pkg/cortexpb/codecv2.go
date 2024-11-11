package cortexpb

import (
	"github.com/prometheus/prometheus/model/labels"
)

// ToLabels return model labels.Labels from timeseries' remote labels.
func (t TimeSeriesV2) ToLabels(b *labels.ScratchBuilder, symbols []string) labels.Labels {
	return desymbolizeLabels(b, t.GetLabelsRefs(), symbols)
}

// ToLabels return model labels.Labels from exemplar remote labels.
func (e ExemplarV2) ToLabels(b *labels.ScratchBuilder, symbols []string) labels.Labels {
	return desymbolizeLabels(b, e.GetLabelsRefs(), symbols)
}

func (m MetadataV2) ToV1Metadata(name string, symbols []string) *MetricMetadata {
	return &MetricMetadata{
		Type:             m.Type,
		MetricFamilyName: name,
		Unit:             symbols[m.UnitRef],
		Help:             symbols[m.HelpRef],
	}
}

// desymbolizeLabels decodes label references, with given symbols to labels.
func desymbolizeLabels(b *labels.ScratchBuilder, labelRefs []uint32, symbols []string) labels.Labels {
	b.Reset()
	for i := 0; i < len(labelRefs); i += 2 {
		b.Add(symbols[labelRefs[i]], symbols[labelRefs[i+1]])
	}
	b.Sort()
	return b.Labels()
}
