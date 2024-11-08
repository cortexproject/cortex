package cortexpbv2

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// ToLabels return model labels.Labels from timeseries' remote labels.
func (t TimeSeries) ToLabels(b *labels.ScratchBuilder, symbols []string) labels.Labels {
	return desymbolizeLabels(b, t.GetLabelsRefs(), symbols)
}

// ToLabels return model labels.Labels from exemplar remote labels.
func (e Exemplar) ToLabels(b *labels.ScratchBuilder, symbols []string) labels.Labels {
	return desymbolizeLabels(b, e.GetLabelsRefs(), symbols)
}

func (m Metadata) ToV1Metadata(name string, symbols []string) *cortexpb.MetricMetadata {
	typ := cortexpb.UNKNOWN

	switch m.Type {
	case METRIC_TYPE_COUNTER:
		typ = cortexpb.COUNTER
	case METRIC_TYPE_GAUGE:
		typ = cortexpb.GAUGE
	case METRIC_TYPE_HISTOGRAM:
		typ = cortexpb.HISTOGRAM
	case METRIC_TYPE_GAUGEHISTOGRAM:
		typ = cortexpb.GAUGEHISTOGRAM
	case METRIC_TYPE_SUMMARY:
		typ = cortexpb.SUMMARY
	case METRIC_TYPE_INFO:
		typ = cortexpb.INFO
	case METRIC_TYPE_STATESET:
		typ = cortexpb.STATESET
	}

	return &cortexpb.MetricMetadata{
		Type:             typ,
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

// IsFloatHistogram returns true if the histogram is float.
func (h Histogram) IsFloatHistogram() bool {
	_, ok := h.GetCount().(*Histogram_CountFloat)
	return ok
}

// FloatHistogramProtoToFloatHistogram extracts a float Histogram from the provided proto message.
func FloatHistogramProtoToFloatHistogram(h Histogram) *histogram.FloatHistogram {
	if !h.IsFloatHistogram() {
		panic("FloatHistogramProtoToFloatHistogram called with an integer histogram")
	}

	return &histogram.FloatHistogram{
		CounterResetHint: histogram.CounterResetHint(h.ResetHint),
		Schema:           h.Schema,
		ZeroThreshold:    h.ZeroThreshold,
		ZeroCount:        h.GetZeroCountFloat(),
		Count:            h.GetCountFloat(),
		Sum:              h.Sum,
		PositiveSpans:    spansProtoToSpans(h.GetPositiveSpans()),
		PositiveBuckets:  h.GetPositiveCounts(),
		NegativeSpans:    spansProtoToSpans(h.GetNegativeSpans()),
		NegativeBuckets:  h.GetNegativeCounts(),
		CustomValues:     h.GetCustomValues(),
	}
}

func HistogramProtoToHistogram(h Histogram) *histogram.Histogram {
	if h.IsFloatHistogram() {
		panic("HistogramProtoToHistogram called with a float histogram")
	}

	return &histogram.Histogram{
		CounterResetHint: histogram.CounterResetHint(h.ResetHint),
		Schema:           h.Schema,
		ZeroThreshold:    h.ZeroThreshold,
		ZeroCount:        h.GetZeroCountInt(),
		Count:            h.GetCountInt(),
		Sum:              h.Sum,
		PositiveSpans:    spansProtoToSpans(h.GetPositiveSpans()),
		PositiveBuckets:  h.GetPositiveDeltas(),
		NegativeSpans:    spansProtoToSpans(h.GetNegativeSpans()),
		NegativeBuckets:  h.GetNegativeDeltas(),
		CustomValues:     h.GetCustomValues(),
	}
}

// HistogramToHistogramProto converts a (normal integer) Histogram to its protobuf message type.
// Changed from https://github.com/prometheus/prometheus/blob/0ab95536115adfe50af249d36d73674be694ca3f/storage/remote/codec.go#L709-L723
func HistogramToHistogramProto(timestamp int64, h *histogram.Histogram) Histogram {
	return Histogram{
		Count:          &Histogram_CountInt{CountInt: h.Count},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount},
		NegativeSpans:  spansToSpansProto(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		ResetHint:      Histogram_ResetHint(h.CounterResetHint),
		Timestamp:      timestamp,
		CustomValues:   h.CustomValues,
	}
}

// FloatHistogramToHistogramProto converts a float Histogram to a normal
// Histogram's protobuf message type.
// Changed from https://github.com/prometheus/prometheus/blob/0ab95536115adfe50af249d36d73674be694ca3f/storage/remote/codec.go#L725-L739
func FloatHistogramToHistogramProto(timestamp int64, fh *histogram.FloatHistogram) Histogram {
	return Histogram{
		Count:          &Histogram_CountFloat{CountFloat: fh.Count},
		Sum:            fh.Sum,
		Schema:         fh.Schema,
		ZeroThreshold:  fh.ZeroThreshold,
		ZeroCount:      &Histogram_ZeroCountFloat{ZeroCountFloat: fh.ZeroCount},
		NegativeSpans:  spansToSpansProto(fh.NegativeSpans),
		NegativeCounts: fh.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(fh.PositiveSpans),
		PositiveCounts: fh.PositiveBuckets,
		ResetHint:      Histogram_ResetHint(fh.CounterResetHint),
		Timestamp:      timestamp,
		CustomValues:   fh.CustomValues,
	}
}

func spansProtoToSpans(s []BucketSpan) []histogram.Span {
	spans := make([]histogram.Span, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = histogram.Span{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}

func spansToSpansProto(s []histogram.Span) []BucketSpan {
	spans := make([]BucketSpan, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = BucketSpan{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}
