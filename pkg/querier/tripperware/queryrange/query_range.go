package queryrange

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

	"github.com/gogo/protobuf/proto"

	"github.com/gogo/status"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// StatusSuccess Prometheus success result.
const StatusSuccess = "success"

type Compression string

const (
	DisableCompression Compression = ""
	GzipCompression    Compression = "gzip"
	SnappyCompression  Compression = "snappy"
	applicationProtobuf string = "application/x-protobuf"
	applicationJson     string = "application/json"
)

var (
	matrix = model.ValMatrix.String()
	json   = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: false,
	}.Froze()
	errEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "end timestamp must not be before start time")
	errNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "zero or negative query resolution step widths are not accepted. Try a positive integer")
	errStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")

	// Name of the cache control header.
	cacheControlHeader = "Cache-Control"
)

type prometheusCodec struct {
	sharded        bool
	compression    Compression
	enableProtobuf bool
}

func NewPrometheusCodec(sharded bool, c string, enableProtobuf bool) *prometheusCodec { //nolint:revive
	var compression Compression
	if c == "gzip" || c == "snappy" {
		compression = Compression(c)
	} else {
		compression = DisableCompression
	}
	return &prometheusCodec{
		sharded:        sharded,
		compression:    compression,
		enableProtobuf: enableProtobuf,
	}
}

// WithStartEnd clones the current `PrometheusRequest` with a new `start` and `end` timestamp.
func (q *PrometheusRequest) WithStartEnd(start int64, end int64) tripperware.Request {
	new := *q
	new.Start = start
	new.End = end
	return &new
}

// WithQuery clones the current `PrometheusRequest` with a new query.
func (q *PrometheusRequest) WithQuery(query string) tripperware.Request {
	new := *q
	new.Query = query
	return &new
}

// WithStats clones the current `PrometheusRequest` with a new stats.
func (q *PrometheusRequest) WithStats(stats string) tripperware.Request {
	new := *q
	new.Stats = stats
	return &new
}

// LogToSpan logs the current `PrometheusRequest` parameters to the specified span.
func (q *PrometheusRequest) LogToSpan(sp opentracing.Span) {
	sp.LogFields(
		otlog.String("query", q.GetQuery()),
		otlog.String("start", timestamp.Time(q.GetStart()).String()),
		otlog.String("end", timestamp.Time(q.GetEnd()).String()),
		otlog.Int64("step (ms)", q.GetStep()),
	)
}

type byFirstTime []*PrometheusResponse

func (a byFirstTime) Len() int           { return len(a) }
func (a byFirstTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFirstTime) Less(i, j int) bool { return a[i].minTime() < a[j].minTime() }

func (resp *PrometheusResponse) minTime() int64 {
	result := resp.Data.Result
	if len(result) == 0 {
		return -1
	}
	if len(result[0].Samples) == 0 {
		return -1
	}
	return result[0].Samples[0].TimestampMs
}

func (resp *PrometheusResponse) HTTPHeaders() map[string][]string {
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

// NewEmptyPrometheusResponse returns an empty successful Prometheus query range response.
func NewEmptyPrometheusResponse() *PrometheusResponse {
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result:     []tripperware.SampleStream{},
		},
	}
}

func (c prometheusCodec) MergeResponse(ctx context.Context, _ tripperware.Request, responses ...tripperware.Response) (tripperware.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "QueryRangeResponse.MergeResponse")
	sp.SetTag("response_count", len(responses))
	defer sp.Finish()
	if len(responses) == 0 {
		return NewEmptyPrometheusResponse(), nil
	}

	promResponses := make([]*PrometheusResponse, 0, len(responses))
	warnings := make([][]string, 0, len(responses))
	for _, res := range responses {
		promResponses = append(promResponses, res.(*PrometheusResponse))
		if w := res.(*PrometheusResponse).Warnings; w != nil {
			warnings = append(warnings, w)
		}
	}

	// Merge the responses.
	sort.Sort(byFirstTime(promResponses))
	sampleStreams, err := matrixMerge(ctx, promResponses)
	if err != nil {
		return nil, err
	}

	response := PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result:     sampleStreams,
			Stats:      statsMerge(c.sharded, promResponses),
		},
		Warnings: strutil.MergeUnsortedSlices(warnings...),
	}

	return &response, nil
}

func (c prometheusCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (tripperware.Request, error) {
	var result PrometheusRequest
	var err error
	result.Start, err = util.ParseTime(r.FormValue("start"))
	if err != nil {
		return nil, decorateWithParamName(err, "start")
	}

	result.End, err = util.ParseTime(r.FormValue("end"))
	if err != nil {
		return nil, decorateWithParamName(err, "end")
	}

	if result.End < result.Start {
		return nil, errEndBeforeStart
	}

	result.Step, err = util.ParseDurationMs(r.FormValue("step"))
	if err != nil {
		return nil, decorateWithParamName(err, "step")
	}

	if result.Step <= 0 {
		return nil, errNegativeStep
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (result.End-result.Start)/result.Step > 11000 {
		return nil, errStepTooSmall
	}

	result.Query = r.FormValue("query")
	result.Stats = r.FormValue("stats")
	result.Path = r.URL.Path

	// Include the specified headers from http request in prometheusRequest.
	for _, header := range forwardHeaders {
		for h, hv := range r.Header {
			if strings.EqualFold(h, header) {
				result.Headers = append(result.Headers, &tripperware.PrometheusRequestHeader{Name: h, Values: hv})
				break
			}
		}
	}

	for _, value := range r.Header.Values(cacheControlHeader) {
		if strings.Contains(value, noStoreValue) {
			result.CachingOptions.Disabled = true
			break
		}
	}

	return &result, nil
}

func (c prometheusCodec) EncodeRequest(ctx context.Context, r tripperware.Request) (*http.Request, error) {
	promReq, ok := r.(*PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}
	params := url.Values{
		"start": []string{tripperware.EncodeTime(promReq.Start)},
		"end":   []string{tripperware.EncodeTime(promReq.End)},
		"step":  []string{encodeDurationMs(promReq.Step)},
		"query": []string{promReq.Query},
		"stats": []string{promReq.Stats},
	}
	u := &url.URL{
		Path:     promReq.Path,
		RawQuery: params.Encode(),
	}
	var h = http.Header{}

	for _, hv := range promReq.Headers {
		for _, v := range hv.Values {
			h.Add(hv.Name, v)
		}
	}

	if c.compression == SnappyCompression || c.compression == GzipCompression {
		h.Set("Accept-Encoding", string(c.compression))
	}
	if c.enableProtobuf {
		h.Set("Accept", applicationProtobuf)
	} else {
		h.Set("Accept", applicationJson)
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

func (c prometheusCodec) DecodeResponse(ctx context.Context, r *http.Response, _ tripperware.Request) (tripperware.Response, error) {
	log, ctx := spanlogger.New(ctx, "ParseQueryRangeResponse") //nolint:ineffassign,staticcheck
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
	log.LogFields(otlog.Int("bytes", len(buf)))

	var resp PrometheusResponse
	if r.Header != nil && r.Header.Get("Content-Type") == applicationProtobuf {
		err = proto.Unmarshal(buf, &resp)
	} else {
		err = json.Unmarshal(buf, &resp)
	}

	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &tripperware.PrometheusResponseHeader{Name: h, Values: hv})
	}
	return &resp, nil
}

func (prometheusCodec) EncodeResponse(ctx context.Context, res tripperware.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*PrometheusResponse)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}

	sp.LogFields(otlog.Int("series", len(a.Data.Result)))

	b, err := json.Marshal(a)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
	}

	sp.LogFields(otlog.Int("bytes", len(b)))

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{applicationJson},
		},
		Body:          io.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
}

// statsMerge merge the stats from 2 responses
// this function is similar to matrixMerge
func statsMerge(shouldSumStats bool, resps []*PrometheusResponse) *tripperware.PrometheusResponseStats {
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
			if shouldSumStats {
				if stats, ok := output[s.GetTimestampMs()]; ok {
					stats.Value += s.Value
				} else {
					output[s.GetTimestampMs()] = s
				}
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

func matrixMerge(ctx context.Context, resps []*PrometheusResponse) ([]tripperware.SampleStream, error) {
	output := make(map[string]tripperware.SampleStream)
	for _, resp := range resps {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if resp == nil {
			continue
		}
		tripperware.MergeSampleStreams(output, resp.Data.GetResult())
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

func encodeDurationMs(d int64) string {
	return strconv.FormatFloat(float64(d)/float64(time.Second/time.Millisecond), 'f', -1, 64)
}

func decorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q; %v"
	if status, ok := status.FromError(err); ok {
		return httpgrpc.Errorf(int(status.Code()), errTmpl, field, status.Message())
	}
	return fmt.Errorf(errTmpl, field, err)
}
