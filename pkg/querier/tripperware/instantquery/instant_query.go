package instantquery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/strutil"
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

	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: false,
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
	result.Time, err = util.ParseTimeParam(r, "time", c.now().Unix())
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

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	buf, err := tripperware.BodyBuffer(r, log)
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

func (instantQueryCodec) MergeResponse(ctx context.Context, req tripperware.Request, responses ...tripperware.Response) (tripperware.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "PrometheusInstantQueryResponse.MergeResponse")
	sp.SetTag("response_count", len(responses))
	defer sp.Finish()

	if len(responses) == 0 {
		return NewEmptyPrometheusInstantQueryResponse(), nil
	} else if len(responses) == 1 {
		return responses[0], nil
	}

	promResponses := make([]*PrometheusInstantQueryResponse, 0, len(responses))
	warnings := make([][]string, 0, len(responses))
	for _, resp := range responses {
		promResponses = append(promResponses, resp.(*PrometheusInstantQueryResponse))
		if w := resp.(*PrometheusInstantQueryResponse).Warnings; w != nil {
			warnings = append(warnings, w)
		}
	}

	var data PrometheusInstantQueryData
	// For now, we only shard queries that returns a vector.
	switch promResponses[0].Data.ResultType {
	case model.ValVector.String():
		v, err := vectorMerge(ctx, req, promResponses)
		if err != nil {
			return nil, err
		}
		data = PrometheusInstantQueryData{
			ResultType: model.ValVector.String(),
			Result: PrometheusInstantQueryResult{
				Result: &PrometheusInstantQueryResult_Vector{
					Vector: v,
				},
			},
			Stats: statsMerge(promResponses),
		}
	case model.ValMatrix.String():
		sampleStreams, err := matrixMerge(ctx, promResponses)
		if err != nil {
			return nil, err
		}

		data = PrometheusInstantQueryData{
			ResultType: model.ValMatrix.String(),
			Result: PrometheusInstantQueryResult{
				Result: &PrometheusInstantQueryResult_Matrix{
					Matrix: &Matrix{
						SampleStreams: sampleStreams,
					},
				},
			},
			Stats: statsMerge(promResponses),
		}
	default:
		return nil, fmt.Errorf("unexpected result type on instant query: %s", promResponses[0].Data.ResultType)
	}

	res := &PrometheusInstantQueryResponse{
		Status:   queryrange.StatusSuccess,
		Data:     data,
		Warnings: strutil.MergeUnsortedSlices(warnings...),
	}
	return res, nil
}

func vectorMerge(ctx context.Context, req tripperware.Request, resps []*PrometheusInstantQueryResponse) (*Vector, error) {
	output := map[string]*Sample{}
	metrics := []string{} // Used to preserve the order for topk and bottomk.
	sortPlan, err := sortPlanForQuery(req.GetQuery())
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1024)
	for _, resp := range resps {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
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
			metric := string(cortexpb.FromLabelAdaptersToLabels(sample.Labels).Bytes(buf))
			if existingSample, ok := output[metric]; !ok {
				output[metric] = s
				metrics = append(metrics, metric) // Preserve the order of metric.
			} else if existingSample.GetTimestampMs() < s.GetTimestampMs() {
				// Choose the latest sample if we see overlap.
				output[metric] = s
			}
		}
	}

	result := &Vector{
		Samples: make([]*Sample, 0, len(output)),
	}

	if len(output) == 0 {
		return result, nil
	}

	if sortPlan == mergeOnly {
		for _, k := range metrics {
			result.Samples = append(result.Samples, output[k])
		}
		return result, nil
	}

	samples := make([]*pair, 0, len(output))
	for k, v := range output {
		samples = append(samples, &pair{
			metric: k,
			s:      v,
		})
	}

	// TODO: What if we have mixed float and histogram samples in the response?
	// Then the sorting behavior is undefined. Prometheus doesn't handle it.
	sort.Slice(samples, func(i, j int) bool {
		// Order is determined by vector.
		switch sortPlan {
		case sortByValuesAsc:
			return getSortValueFromPair(samples, i) < getSortValueFromPair(samples, j)
		case sortByValuesDesc:
			return getSortValueFromPair(samples, i) > getSortValueFromPair(samples, j)
		}
		return samples[i].metric < samples[j].metric
	})

	for _, p := range samples {
		result.Samples = append(result.Samples, p.s)
	}
	return result, nil
}

type sortPlan int

const (
	mergeOnly        sortPlan = 0
	sortByValuesAsc  sortPlan = 1
	sortByValuesDesc sortPlan = 2
	sortByLabels     sortPlan = 3
)

type pair struct {
	metric string
	s      *Sample
}

// getSortValueFromPair gets the float value used for sorting from samples.
// If float sample, use sample value. If histogram sample, use histogram sum.
// This is the same behavior as Prometheus https://github.com/prometheus/prometheus/blob/v2.53.0/promql/functions.go#L1595.
func getSortValueFromPair(samples []*pair, i int) float64 {
	if samples[i].s.Histogram != nil {
		return samples[i].s.Histogram.Histogram.Sum
	}
	// Impossible to have both histogram and sample nil.
	return samples[i].s.Sample.Value
}

func sortPlanForQuery(q string) (sortPlan, error) {
	expr, err := promqlparser.ParseExpr(q)
	if err != nil {
		return 0, err
	}
	// Check if the root expression is topk or bottomk
	if aggr, ok := expr.(*promqlparser.AggregateExpr); ok {
		if aggr.Op == promqlparser.TOPK || aggr.Op == promqlparser.BOTTOMK {
			return mergeOnly, nil
		}
	}
	checkForSort := func(expr promqlparser.Expr) (sortAsc, sortDesc bool) {
		if n, ok := expr.(*promqlparser.Call); ok {
			if n.Func != nil {
				if n.Func.Name == "sort" {
					sortAsc = true
				}
				if n.Func.Name == "sort_desc" {
					sortDesc = true
				}
			}
		}
		return sortAsc, sortDesc
	}
	// Check the root expression for sort
	if sortAsc, sortDesc := checkForSort(expr); sortAsc || sortDesc {
		if sortAsc {
			return sortByValuesAsc, nil
		}
		return sortByValuesDesc, nil
	}

	// If the root expression is a binary expression, check the LHS and RHS for sort
	if bin, ok := expr.(*promqlparser.BinaryExpr); ok {
		if sortAsc, sortDesc := checkForSort(bin.LHS); sortAsc || sortDesc {
			if sortAsc {
				return sortByValuesAsc, nil
			}
			return sortByValuesDesc, nil
		}
		if sortAsc, sortDesc := checkForSort(bin.RHS); sortAsc || sortDesc {
			if sortAsc {
				return sortByValuesAsc, nil
			}
			return sortByValuesDesc, nil
		}
	}
	return sortByLabels, nil
}

func matrixMerge(ctx context.Context, resps []*PrometheusInstantQueryResponse) ([]tripperware.SampleStream, error) {
	output := make(map[string]tripperware.SampleStream)
	for _, resp := range resps {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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

	return result, nil
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

func init() {
	jsoniter.RegisterTypeEncoderFunc("instantquery.Sample", encodeSample, marshalJSONIsEmpty)
	jsoniter.RegisterTypeDecoderFunc("instantquery.Sample", decodeSample)
}

func marshalJSONIsEmpty(ptr unsafe.Pointer) bool {
	return false
}

func decodeSample(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	ss := (*Sample)(ptr)
	for field := iter.ReadObject(); field != ""; field = iter.ReadObject() {
		switch field {
		case "metric":
			metricString := iter.ReadAny().ToString()
			lbls := labels.Labels{}
			if err := json.UnmarshalFromString(metricString, &lbls); err != nil {
				iter.ReportError("unmarshal Sample", err.Error())
				return
			}
			ss.Labels = cortexpb.FromLabelsToLabelAdapters(lbls)
		case "value":
			ss.Sample = &cortexpb.Sample{}
			cortexpb.SampleJsoniterDecode(unsafe.Pointer(ss.Sample), iter)
		case "histogram":
			ss.Histogram = &tripperware.SampleHistogramPair{}
			tripperware.UnmarshalSampleHistogramPairJSON(unsafe.Pointer(ss.Histogram), iter)
		default:
			iter.ReportError("unmarshal Sample", fmt.Sprint("unexpected key:", field))
			return
		}
	}
}

func encodeSample(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	ss := (*Sample)(ptr)
	stream.WriteObjectStart()

	stream.WriteObjectField(`metric`)
	lbls, err := cortexpb.FromLabelAdaptersToLabels(ss.Labels).MarshalJSON()
	if err != nil {
		stream.Error = err
		return
	}
	stream.SetBuffer(append(stream.Buffer(), lbls...))

	if ss.Sample != nil {
		stream.WriteMore()
		stream.WriteObjectField(`value`)
		cortexpb.SampleJsoniterEncode(unsafe.Pointer(ss.Sample), stream)
	}

	if ss.Histogram != nil {
		stream.WriteMore()
		stream.WriteObjectField(`histogram`)
		tripperware.MarshalSampleHistogramPairJSON(unsafe.Pointer(ss.Histogram), stream)
	}

	stream.WriteObjectEnd()
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
