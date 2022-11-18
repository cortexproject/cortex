package instantquery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

var (
	InstantQueryCodec tripperware.Codec = newInstantQueryCodec()
	json                                = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
)

type PrometheusRequest struct {
	tripperware.Request
	Time    int64
	Stats   string
	Query   string
	Path    string
	Headers http.Header
}

// GetTime returns time in milliseconds.
func (r *PrometheusRequest) GetTime() int64 {
	return r.Time
}

// GetStart returns always 0 for instant query.
func (r *PrometheusRequest) GetStart() int64 {
	return 0
}

// GetEnd returns always 0 for instant query.
func (r *PrometheusRequest) GetEnd() int64 {
	return 0
}

// GetStep returns always 0 for instant query.
func (r *PrometheusRequest) GetStep() int64 {
	return 0
}

// GetQuery returns the query of the request.
func (r *PrometheusRequest) GetQuery() string {
	return r.Query
}

// WithStartEnd clone the current request with different start and end timestamp.
func (r *PrometheusRequest) WithStartEnd(int64, int64) tripperware.Request {
	return r
}

// WithQuery clone the current request with a different query.
func (r *PrometheusRequest) WithQuery(query string) tripperware.Request {
	q := *r
	q.Query = query
	return &q
}

// LogToSpan writes information about this request to an OpenTracing span
func (r *PrometheusRequest) LogToSpan(sp opentracing.Span) {
	sp.LogFields(
		otlog.String("query", r.GetQuery()),
		otlog.String("time", timestamp.Time(r.GetTime()).String()),
	)
}

// GetStats returns the stats of the request.
func (r *PrometheusRequest) GetStats() string {
	return r.Stats
}

// WithStats clones the current `PrometheusRequest` with a new stats.
func (r *PrometheusRequest) WithStats(stats string) tripperware.Request {
	q := *r
	q.Stats = stats
	return &q
}

type instantQueryCodec struct {
	tripperware.Codec
	now func() time.Time
}

func newInstantQueryCodec() instantQueryCodec {
	return instantQueryCodec{now: time.Now}
}

func (resp *PrometheusInstantQueryResponse) HTTPHeaders() map[string][]string {
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

func (c instantQueryCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (tripperware.Request, error) {
	result := PrometheusRequest{Headers: map[string][]string{}}
	var err error
	result.Time, err = parseTimeParam(r, "time", c.now().Unix())
	if err != nil {
		return nil, decorateWithParamName(err, "time")
	}

	result.Query = r.FormValue("query")
	result.Stats = r.FormValue("stats")
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

func (instantQueryCodec) DecodeResponse(ctx context.Context, r *http.Response, _ tripperware.Request) (tripperware.Response, error) {
	log, ctx := spanlogger.New(ctx, "PrometheusInstantQueryResponse") //nolint:ineffassign,staticcheck
	defer log.Finish()

	buf, err := tripperware.BodyBuffer(r)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if r.StatusCode/100 != 2 {
		return nil, httpgrpc.Errorf(r.StatusCode, string(buf))
	}

	var resp PrometheusInstantQueryResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &tripperware.PrometheusResponseHeader{Name: h, Values: hv})
	}
	return &resp, nil
}

func (instantQueryCodec) EncodeRequest(ctx context.Context, r tripperware.Request) (*http.Request, error) {
	promReq, ok := r.(*PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}
	params := url.Values{
		"time":  []string{tripperware.EncodeTime(promReq.Time)},
		"query": []string{promReq.Query},
	}

	if promReq.Stats != "" {
		params.Add("stats", promReq.Stats)
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

	// Always ask gzip to the querier
	h.Set("Accept-Encoding", "gzip")

	req := &http.Request{
		Method:     "GET",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       http.NoBody,
		Header:     h,
	}

	return req.WithContext(ctx), nil
}

func (instantQueryCodec) EncodeResponse(ctx context.Context, res tripperware.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*PrometheusInstantQueryResponse)
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

func (instantQueryCodec) MergeResponse(responses ...tripperware.Response) (tripperware.Response, error) {
	if len(responses) == 0 {
		return NewEmptyPrometheusInstantQueryResponse(), nil
	} else if len(responses) == 1 {
		return responses[0], nil
	}

	promResponses := make([]*PrometheusInstantQueryResponse, 0, len(responses))
	for _, resp := range responses {
		promResponses = append(promResponses, resp.(*PrometheusInstantQueryResponse))
	}

	var data PrometheusInstantQueryData
	// For now, we only shard queries that returns a vector.
	switch promResponses[0].Data.ResultType {
	case model.ValVector.String():
		data = PrometheusInstantQueryData{
			ResultType: model.ValVector.String(),
			Result: PrometheusInstantQueryResult{
				Result: &PrometheusInstantQueryResult_Vector{
					Vector: vectorMerge(promResponses),
				},
			},
			Stats: statsMerge(promResponses),
		}
	case model.ValMatrix.String():
		data = PrometheusInstantQueryData{
			ResultType: model.ValMatrix.String(),
			Result: PrometheusInstantQueryResult{
				Result: &PrometheusInstantQueryResult_Matrix{
					Matrix: &Matrix{
						SampleStreams: matrixMerge(promResponses),
					},
				},
			},
			Stats: statsMerge(promResponses),
		}
	default:
		return nil, fmt.Errorf("unexpected result type on instant query: %s", promResponses[0].Data.ResultType)
	}

	res := &PrometheusInstantQueryResponse{
		Status: queryrange.StatusSuccess,
		Data:   data,
	}
	return res, nil
}

func vectorMerge(resps []*PrometheusInstantQueryResponse) *Vector {
	output := map[string]*Sample{}
	for _, resp := range resps {
		if resp == nil {
			continue
		}
		// Merge vector result samples only. Skip other types such as
		// string, scalar as those are not sharable.
		if resp.Data.Result.GetVector() == nil {
			continue
		}
		for _, sample := range resp.Data.Result.GetVector().Samples {
			s := sample
			if s == nil {
				continue
			}
			metric := cortexpb.FromLabelAdaptersToLabels(sample.Labels).String()
			if existingSample, ok := output[metric]; !ok {
				output[metric] = s
			} else if existingSample.GetSample().TimestampMs < s.GetSample().TimestampMs {
				// Choose the latest sample if we see overlap.
				output[metric] = s
			}
		}
	}

	if len(output) == 0 {
		return &Vector{
			Samples: make([]*Sample, 0),
		}
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := &Vector{
		Samples: make([]*Sample, 0, len(output)),
	}
	for _, key := range keys {
		result.Samples = append(result.Samples, output[key])
	}
	return result
}

func matrixMerge(resps []*PrometheusInstantQueryResponse) []tripperware.SampleStream {
	output := make(map[string]tripperware.SampleStream)
	for _, resp := range resps {
		if resp == nil {
			continue
		}
		if resp.Data.Result.GetMatrix() == nil {
			continue
		}
		tripperware.MergeSampleStreams(output, resp.Data.Result.GetMatrix().GetSampleStreams())
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]tripperware.SampleStream, 0, len(output))
	for _, key := range keys {
		result = append(result, output[key])
	}

	return result
}

// NewEmptyPrometheusInstantQueryResponse returns an empty successful Prometheus query range response.
func NewEmptyPrometheusInstantQueryResponse() *PrometheusInstantQueryResponse {
	return &PrometheusInstantQueryResponse{
		Status: queryrange.StatusSuccess,
		Data: PrometheusInstantQueryData{
			ResultType: model.ValVector.String(),
			Result: PrometheusInstantQueryResult{
				Result: &PrometheusInstantQueryResult_Vector{},
			},
		},
	}
}

func statsMerge(resps []*PrometheusInstantQueryResponse) *tripperware.PrometheusResponseStats {
	output := map[int64]*tripperware.PrometheusResponseQueryableSamplesStatsPerStep{}
	hasStats := false
	for _, resp := range resps {
		if resp.Data.Stats == nil {
			continue
		}

		hasStats = true
		if resp.Data.Stats.Samples == nil {
			continue
		}

		for _, s := range resp.Data.Stats.Samples.TotalQueryableSamplesPerStep {
			if stats, ok := output[s.GetTimestampMs()]; ok {
				stats.Value += s.Value
			} else {
				output[s.GetTimestampMs()] = s
			}
		}
	}

	if !hasStats {
		return nil
	}

	return tripperware.StatsMerge(output)
}

func decorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q; %v"
	if status, ok := status.FromError(err); ok {
		return httpgrpc.Errorf(int(status.Code()), errTmpl, field, status.Message())
	}
	return fmt.Errorf(errTmpl, field, err)
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Sample) UnmarshalJSON(data []byte) error {
	var sample struct {
		Metric model.Metric    `json:"metric"`
		Value  cortexpb.Sample `json:"value"`
	}
	if err := json.Unmarshal(data, &sample); err != nil {
		return err
	}
	s.Labels = cortexpb.FromMetricsToLabelAdapters(sample.Metric)
	s.Sample = sample.Value
	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *Sample) MarshalJSON() ([]byte, error) {
	sample := struct {
		Metric model.Metric    `json:"metric"`
		Value  cortexpb.Sample `json:"value"`
	}{
		Metric: cortexpb.FromLabelAdaptersToMetric(s.Labels),
		Value:  s.Sample,
	}
	return json.Marshal(sample)
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *PrometheusInstantQueryData) UnmarshalJSON(data []byte) error {
	var queryData struct {
		ResultType string                               `json:"resultType"`
		Stats      *tripperware.PrometheusResponseStats `json:"stats,omitempty"`
	}

	if err := json.Unmarshal(data, &queryData); err != nil {
		return err
	}
	s.ResultType = queryData.ResultType
	s.Stats = queryData.Stats
	switch s.ResultType {
	case model.ValVector.String():
		var result struct {
			Samples []*Sample `json:"result"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}
		s.Result = PrometheusInstantQueryResult{
			Result: &PrometheusInstantQueryResult_Vector{Vector: &Vector{
				Samples: result.Samples,
			}},
		}
	case model.ValMatrix.String():
		var result struct {
			SampleStreams []tripperware.SampleStream `json:"result"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}
		s.Result = PrometheusInstantQueryResult{
			Result: &PrometheusInstantQueryResult_Matrix{Matrix: &Matrix{
				SampleStreams: result.SampleStreams,
			}},
		}
	default:
		s.Result = PrometheusInstantQueryResult{
			Result: &PrometheusInstantQueryResult_RawBytes{data},
		}
	}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *PrometheusInstantQueryData) MarshalJSON() ([]byte, error) {
	switch s.ResultType {
	case model.ValVector.String():
		res := struct {
			ResultType string                               `json:"resultType"`
			Data       []*Sample                            `json:"result"`
			Stats      *tripperware.PrometheusResponseStats `json:"stats,omitempty"`
		}{
			ResultType: s.ResultType,
			Data:       s.Result.GetVector().Samples,
			Stats:      s.Stats,
		}
		return json.Marshal(res)
	case model.ValMatrix.String():
		res := struct {
			ResultType string                               `json:"resultType"`
			Data       []tripperware.SampleStream           `json:"result"`
			Stats      *tripperware.PrometheusResponseStats `json:"stats,omitempty"`
		}{
			ResultType: s.ResultType,
			Data:       s.Result.GetMatrix().SampleStreams,
			Stats:      s.Stats,
		}
		return json.Marshal(res)
	default:
		return s.Result.GetRawBytes(), nil
	}
}

func parseTimeParam(r *http.Request, paramName string, defaultValue int64) (int64, error) {
	val := r.FormValue(paramName)
	if val == "" {
		val = strconv.FormatInt(defaultValue, 10)
	}
	result, err := util.ParseTime(val)
	if err != nil {
		return 0, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}
