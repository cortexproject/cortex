package ruler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type RemoteWritePusher struct {
	u       string
	headers map[string]string
}

func NewRemoteWritePusher(u string, headers map[string]string) *RemoteWritePusher {
	return &RemoteWritePusher{
		u:       u,
		headers: headers,
	}
}

var _ Pusher = &RemoteWritePusher{}

func (r *RemoteWritePusher) Push(ctx context.Context, wr *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	promwr := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, len(wr.Timeseries)),
		Metadata:   make([]prompb.MetricMetadata, 0, len(wr.Metadata)),
	}

	for _, ts := range wr.Timeseries {
		promwr.Timeseries = append(promwr.Timeseries, prompb.TimeSeries{
			Labels:    makeLabels(ts.Labels),
			Samples:   makeSamples(ts.Samples),
			Exemplars: makeExemplars(ts.Exemplars),
			//Histograms: makeHistograms(ts.Histograms),
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.u, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}

	for k, v := range r.headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("got status code: %d", resp.StatusCode)
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

/*
func makeHistograms(in []cortexpb.Histogram) []prompb.Histogram {
	out := make([]prompb.Histogram, 0, len(in))
	for _, h := range in {
		out = append(out, cortexpb.HistogramPromProtoToHistogramProto(h))
	}
	return out
}
*/
