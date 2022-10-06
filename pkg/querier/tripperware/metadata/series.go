package metadata

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
)

type PrometheusSeriesRequest struct {
	tripperware.Request
	Path     string
	Start    int64
	End      int64
	Matchers []string
	Headers  http.Header
}

// WithStartEnd clones the current `PrometheusRequest` with a new `start` and `end` timestamp.
func (r *PrometheusSeriesRequest) WithStartEnd(start int64, end int64) tripperware.Request {
	new := *r
	new.Start = start
	new.End = end
	return &new
}

func (r *PrometheusSeriesRequest) GetStart() int64 {
	return r.Start
}

func (r *PrometheusSeriesRequest) GetEnd() int64 {
	return r.End
}

// GetStep returns always 0 for Series.
func (r *PrometheusSeriesRequest) GetStep() int64 {
	return 0
}

// LogToSpan writes information about this request to an OpenTracing span
func (r *PrometheusSeriesRequest) LogToSpan(sp opentracing.Span) {
	sp.LogFields(
		otlog.String("match[]", fmt.Sprintf("%v", r.Matchers)),
		otlog.String("start", timestamp.Time(r.GetStart()).String()),
		otlog.String("end", timestamp.Time(r.GetEnd()).String()),
	)
}

type seriesCodec struct {
	tripperware.Codec
	now      func() time.Time
	lookback time.Duration
}

func NewSeriesCodec(cfg queryrange.Config) seriesCodec {
	return seriesCodec{
		now:      time.Now,
		lookback: cfg.SplitMetadataLookback,
	}
}

func (resp *PrometheusSeriesResponse) HTTPHeaders() map[string][]string {
	if resp != nil && resp.GetHeaders() != nil {
		r := map[string][]string{}
		for _, header := range resp.GetHeaders() {
			if header != nil {
				r[header.Name] = header.Values
			}
		}

		return r
	}
	return nil
}

func (c seriesCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (tripperware.Request, error) {
	result := PrometheusSeriesRequest{Headers: map[string][]string{}}
	var err error
	result.Start, err = tripperware.ParseTimeParam(r, "start", c.now().Add(-c.lookback).Unix())
	if err != nil {
		return nil, tripperware.DecorateWithParamName(err, "start")
	}
	result.End, err = tripperware.ParseTimeParam(r, "end", c.now().Add(5*time.Minute).Unix())
	if err != nil {
		return nil, tripperware.DecorateWithParamName(err, "end")
	}

	result.Matchers = r.Form["match[]"]
	result.Path = r.URL.Path
	// Include the specified headers from http request in prometheusRequest.
	for _, header := range forwardHeaders {
		for h, hv := range r.Header {
			if strings.EqualFold(h, header) {
				result.Headers[h] = hv
				break
			}
		}
	}

	return &result, nil
}

func (seriesCodec) DecodeResponse(ctx context.Context, r *http.Response, _ tripperware.Request) (tripperware.Response, error) {
	if r.StatusCode/100 != 2 {
		body, _ := io.ReadAll(r.Body)
		return nil, httpgrpc.Errorf(r.StatusCode, string(body))
	}

	log, _ := spanlogger.New(ctx, "PrometheusSeriesResponse") //nolint:ineffassign,staticcheck
	defer log.Finish()

	buf, err := tripperware.BodyBuffer(r)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	var resp PrometheusSeriesResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &tripperware.PrometheusResponseHeader{Name: h, Values: hv})
	}
	return &resp, nil
}

func (seriesCodec) EncodeRequest(ctx context.Context, r tripperware.Request) (*http.Request, error) {
	promReq, ok := r.(*PrometheusSeriesRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}
	params := url.Values{
		"start":   []string{tripperware.EncodeTime(promReq.Start)},
		"end":     []string{tripperware.EncodeTime(promReq.End)},
		"match[]": promReq.Matchers,
	}

	u := &url.URL{
		Path:     promReq.Path,
		RawQuery: params.Encode(),
	}

	var h = http.Header{}

	for n, hv := range promReq.Headers {
		for _, v := range hv {
			h.Add(n, v)
		}
	}

	req := &http.Request{
		Method:     "GET",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       http.NoBody,
		Header:     h,
	}

	return req.WithContext(ctx), nil
}

func (seriesCodec) EncodeResponse(ctx context.Context, res tripperware.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*PrometheusSeriesResponse)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}

	b, err := json.Marshal(a)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
	}

	sp.LogFields(otlog.Int("bytes", len(b)))

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:          io.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
}

func (seriesCodec) MergeResponse(responses ...tripperware.Response) (tripperware.Response, error) {
	if len(responses) == 0 {
		return NewEmptyPrometheusSeriesResponse(), nil
	} else if len(responses) == 1 {
		return responses[0], nil
	}

	promResponses := make([]*PrometheusSeriesResponse, 0, len(responses))
	for _, resp := range responses {
		promResponses = append(promResponses, resp.(*PrometheusSeriesResponse))
	}

	metricsSet := make(map[model.Fingerprint]model.LabelSet)
	for _, promResp := range promResponses {
		for _, metric := range promResp.Data {
			ls := model.LabelSet{}
			for k, v := range metric.Metric {
				ls[model.LabelName(k)] = model.LabelValue(v)
			}
			fingerprint := ls.Fingerprint()
			metricsSet[fingerprint] = ls
		}
	}

	result := []Metric{}
	for _, m := range metricsSet {
		metric := map[string]string{}
		for k, v := range m {
			metric[string(k)] = string(v)
		}
		result = append(result, Metric{Metric: metric})
	}

	res := &PrometheusSeriesResponse{
		Status: queryrange.StatusSuccess,
		Data:   result,
	}
	return res, nil
}

// NewEmptyPrometheusSeriesResponse returns an empty successful Prometheus query range response.
func NewEmptyPrometheusSeriesResponse() *PrometheusSeriesResponse {
	return &PrometheusSeriesResponse{
		Status: queryrange.StatusSuccess,
		Data:   []Metric{},
	}
}
