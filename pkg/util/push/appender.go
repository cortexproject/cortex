package push

import (
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

// collectingAppender implements storage.AppenderV2 for the OTLP translator by
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
// storage.AppenderV2 and accumulates all appended data
// for later retrieval via TimeSeries() and Metadata().
func newCollectingAppender() *collectingAppender {
	return &collectingAppender{
		series:   make(map[string]*collectedSeries),
		metadata: make(map[string]prompb.MetricMetadata),
	}
}

// Append records one sample. The start timestamp is dropped: prompb v1 cannot carry it.
func (c *collectingAppender) Append(_ storage.SeriesRef, ls labels.Labels, _, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AppendV2Options) (storage.SeriesRef, error) {
	c.recordMetadata(opts.MetricFamilyName, opts.Metadata)
	s := c.getOrCreateSeries(ls)
	switch {
	case fh != nil:
		s.histograms = append(s.histograms, prompb.FromFloatHistogram(t, fh))
	case h != nil:
		s.histograms = append(s.histograms, prompb.FromIntHistogram(t, h))
	default:
		s.samples = append(s.samples, prompb.Sample{Value: v, Timestamp: t})
	}
	for _, e := range opts.Exemplars {
		s.exemplars = append(s.exemplars, exemplarToProm(e))
	}
	return 0, nil
}

// Commit is a no-op: the collected data is read with TimeSeries and Metadata.
func (c *collectingAppender) Commit() error { return nil }

// Rollback is a no-op for the same reason as Commit.
func (c *collectingAppender) Rollback() error { return nil }

func (c *collectingAppender) recordMetadata(family string, meta metadata.Metadata) {
	if family == "" {
		return
	}
	if _, ok := c.metadata[family]; ok {
		return
	}
	c.metadata[family] = prompb.MetricMetadata{
		Type:             prompb.FromMetadataType(meta.Type),
		MetricFamilyName: family,
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
