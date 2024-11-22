package ruler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/tenant"

	"github.com/klauspost/compress/snappy"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"

	"github.com/projectdiscovery/retryablehttp-go"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/prometheus/prometheus/util/annotations"
)

type FrontendQueryable struct {
	p                   *client.Pool
	log                 log.Logger
	restoreIgnoreLabels []string
}

func NewFrontendQueryable(frontendPool *client.Pool, log log.Logger, restoreIgnoreLabels []string) *FrontendQueryable {
	return &FrontendQueryable{
		p:                   frontendPool,
		log:                 log,
		restoreIgnoreLabels: restoreIgnoreLabels,
	}
}

func (q *FrontendQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &FrontendQuerier{
		mint:                mint,
		maxt:                maxt,
		p:                   q.p,
		Logger:              q.log,
		restoreIgnoreLabels: q.restoreIgnoreLabels,
	}, nil
}

type FrontendQuerier struct {
	mint, maxt          int64
	p                   *client.Pool
	frontendAddr        string
	Logger              log.Logger
	restoreIgnoreLabels []string
}

func (f *FrontendQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	cl, err := f.p.GetClientFor(f.frontendAddr)
	if err != nil {
		level.Error(f.Logger).Log("msg", "failed to get client", "err", err)
		return storage.EmptySeriesSet()
	}

	query := storepb.PromMatchersToString(matchers...)

	c := cl.(*frontendClient)

	var step float64 = 30
	if hints != nil {
		step = float64(hints.Step)
	}

	m, err := c.RangeQuery(ctx, query, timestamp.Time(f.mint), timestamp.Time(f.maxt), step)
	if err != nil {
		level.Error(f.Logger).Log("msg", "failed to query", "err", err)
		return storage.EmptySeriesSet()
	}

	matrix := make([]*model.SampleStream, 0, m.Len())
	for _, metric := range m {
		for _, label := range f.restoreIgnoreLabels {
			delete(metric.Metric, model.LabelName(label))
		}

		matrix = append(matrix, &model.SampleStream{
			Metric: metric.Metric,
			Values: metric.Values,
		})
	}

	return series.MatrixToSeriesSet(sortSeries, matrix)
}

func (f *FrontendQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (f *FrontendQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (f *FrontendQuerier) Close() error {
	return nil
}

type RemoteWritePusher struct {
	u       string
	headers map[string]string
	client  *http.Client
}

func NewRemoteWritePusher(u string, headers map[string]string) *RemoteWritePusher {
	client := retryablehttp.NewClient(retryablehttp.DefaultOptionsSingle)

	return &RemoteWritePusher{
		u:       u,
		headers: headers,
		client:  client.HTTPClient,
	}
}

const orgIDKey = "{org_id}"

var _ Pusher = &RemoteWritePusher{}

func (r *RemoteWritePusher) Push(ctx context.Context, wr *cortexpb.WriteRequest) (wresp *cortexpb.WriteResponse, rerr error) {
	if len(wr.Timeseries) == 0 && len(wr.Metadata) == 0 {
		return &cortexpb.WriteResponse{}, nil
	}

	promwr := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, len(wr.Timeseries)),
		Metadata:   make([]prompb.MetricMetadata, 0, len(wr.Metadata)),
	}

	orgID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	for _, ts := range wr.Timeseries {
		promwr.Timeseries = append(promwr.Timeseries, prompb.TimeSeries{
			Labels:     makeLabels(ts.Labels),
			Samples:    makeSamples(ts.Samples),
			Exemplars:  makeExemplars(ts.Exemplars),
			Histograms: makeHistograms(ts.Histograms),
		})
	}

	for _, m := range wr.Metadata {
		promwr.Metadata = append(promwr.Metadata, prompb.MetricMetadata{
			Type:             prompb.MetricMetadata_MetricType(m.Type),
			Unit:             m.Unit,
			Help:             m.Help,
			MetricFamilyName: m.MetricFamilyName,
		})
	}

	m, err := promwr.Marshal()
	if err != nil {
		return nil, err
	}

	encoded := snappy.Encode(nil, m)

	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, r.u, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}

	for k, v := range r.headers {
		v = strings.ReplaceAll(v, orgIDKey, orgID)

		req.Header[k] = []string{v}
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer runutil.ExhaustCloseWithErrCapture(&rerr, resp.Body, "remote_write response body")

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("got status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return &cortexpb.WriteResponse{}, nil
}

func makeLabels(in []cortexpb.LabelAdapter) []prompb.Label {
	out := make([]prompb.Label, 0, len(in))
	for _, l := range in {
		out = append(out, prompb.Label{Name: l.Name, Value: l.Value})
	}
	return out
}

func makeSamples(in []cortexpb.Sample) []prompb.Sample {
	out := make([]prompb.Sample, 0, len(in))
	for _, s := range in {
		out = append(out, prompb.Sample{
			Value:     s.Value,
			Timestamp: s.TimestampMs,
		})
	}
	return out
}

func makeExemplars(in []cortexpb.Exemplar) []prompb.Exemplar {
	out := make([]prompb.Exemplar, 0, len(in))
	for _, e := range in {
		out = append(out, prompb.Exemplar{
			Labels:    makeLabels(e.Labels),
			Value:     e.Value,
			Timestamp: e.TimestampMs,
		})
	}
	return out
}

func makeHistograms(in []cortexpb.Histogram) []prompb.Histogram {
	out := make([]prompb.Histogram, 0, len(in))
	for _, h := range in {
		ph := prompb.Histogram{
			Sum:           h.Sum,
			Schema:        h.Schema,
			ZeroThreshold: h.ZeroThreshold,
		}

		if v, ok := h.Count.(*cortexpb.Histogram_CountFloat); ok {
			ph.Count = &prompb.Histogram_CountFloat{CountFloat: v.CountFloat}
		}
		if v, ok := h.Count.(*cortexpb.Histogram_CountInt); ok {
			ph.Count = &prompb.Histogram_CountInt{CountInt: v.CountInt}
		}
		if v, ok := h.ZeroCount.(*cortexpb.Histogram_ZeroCountFloat); ok {
			ph.ZeroCount = &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: v.ZeroCountFloat}
		}
		if v, ok := h.ZeroCount.(*cortexpb.Histogram_ZeroCountInt); ok {
			ph.ZeroCount = &prompb.Histogram_ZeroCountInt{ZeroCountInt: v.ZeroCountInt}
		}
		for _, ns := range h.NegativeSpans {
			ph.NegativeSpans = append(ph.NegativeSpans, prompb.BucketSpan{
				Offset: ns.Offset,
				Length: ns.Length,
			})
		}
		ph.NegativeDeltas = append(ph.NegativeDeltas, h.NegativeDeltas...)
		ph.NegativeCounts = append(ph.NegativeCounts, h.NegativeCounts...)

		for _, ps := range h.PositiveSpans {
			ph.PositiveSpans = append(ph.PositiveSpans, prompb.BucketSpan{
				Offset: ps.Offset,
				Length: ps.Length,
			})
		}

		ph.PositiveDeltas = append(ph.PositiveDeltas, h.PositiveDeltas...)
		ph.PositiveCounts = append(ph.PositiveCounts, h.PositiveCounts...)

		ph.ResetHint = prompb.Histogram_ResetHint(h.ResetHint)
		ph.Timestamp = h.TimestampMs

		out = append(out, ph)
	}
	return out
}
