package push

import (
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
)

// collectingAppender implements prometheusremotewrite.CombinedAppender by
// accumulating samples, histograms, exemplars and metadata into buffers that
// can be converted to prompb TimeSeries and MetricMetadata.
type collectingAppender struct {
	series   map[string]*collectedSeries
	metadata map[string]prompb.MetricMetadata
}

type collectedSeries struct {
	labels     labels.Labels
	samples    []prompb.Sample
	exemplars  []prompb.Exemplar
	histograms []prompb.Histogram
}

// newCollectingAppender returns a new collectingAppender that implements
// prometheusremotewrite.CombinedAppender and accumulates all appended data
// for later retrieval via TimeSeries() and Metadata().
func newCollectingAppender() *collectingAppender {
	return &collectingAppender{
		series:   make(map[string]*collectedSeries),
		metadata: make(map[string]prompb.MetricMetadata),
	}
}

func (c *collectingAppender) AppendSample(ls labels.Labels, meta prometheusremotewrite.Metadata, ct, t int64, v float64, es []exemplar.Exemplar) error {
	c.recordMetadata(meta)
	s := c.getOrCreateSeries(ls)
	s.samples = append(s.samples, prompb.Sample{Value: v, Timestamp: t})
	for _, e := range es {
		s.exemplars = append(s.exemplars, exemplarToProm(e))
	}
	return nil
}

func (c *collectingAppender) AppendHistogram(ls labels.Labels, meta prometheusremotewrite.Metadata, ct, t int64, h *histogram.Histogram, es []exemplar.Exemplar) error {
	if h == nil {
		return nil
	}
	c.recordMetadata(meta)
	s := c.getOrCreateSeries(ls)
	s.histograms = append(s.histograms, prompb.FromIntHistogram(t, h))
	for _, e := range es {
		s.exemplars = append(s.exemplars, exemplarToProm(e))
	}
	return nil
}

func (c *collectingAppender) recordMetadata(meta prometheusremotewrite.Metadata) {
	key := meta.MetricFamilyName
	if key == "" {
		return
	}
	if _, ok := c.metadata[key]; ok {
		return
	}
	c.metadata[key] = prompb.MetricMetadata{
		Type:             prompb.FromMetadataType(meta.Type),
		MetricFamilyName: meta.MetricFamilyName,
		Help:             meta.Help,
		Unit:             meta.Unit,
	}
}

func (c *collectingAppender) getOrCreateSeries(ls labels.Labels) *collectedSeries {
	key := ls.String()
	if s, ok := c.series[key]; ok {
		return s
	}
	s := &collectedSeries{labels: labels.NewBuilder(ls).Labels()}
	c.series[key] = s
	return s
}

func exemplarToProm(e exemplar.Exemplar) prompb.Exemplar {
	return prompb.Exemplar{
		Labels:    prompb.FromLabels(e.Labels, nil),
		Value:     e.Value,
		Timestamp: e.Ts,
	}
}

// TimeSeries returns the accumulated time series in prompb form.
func (c *collectingAppender) TimeSeries() []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, 0, len(c.series))
	for _, s := range c.series {
		ts := prompb.TimeSeries{
			Labels:     prompb.FromLabels(s.labels, nil),
			Samples:    s.samples,
			Exemplars:  s.exemplars,
			Histograms: s.histograms,
		}
		out = append(out, ts)
	}
	return out
}

// Metadata returns the accumulated metric metadata in prompb form.
func (c *collectingAppender) Metadata() []prompb.MetricMetadata {
	out := make([]prompb.MetricMetadata, 0, len(c.metadata))
	for _, m := range c.metadata {
		out = append(out, m)
	}
	return out
}
