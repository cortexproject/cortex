package queryrange

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/status"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// StatusSuccess Prometheus success result.
const StatusSuccess = "success"

var (
	matrix = model.ValMatrix.String()
	json   = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: false,
	}.Froze()
	errEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "%s", "end timestamp must not be before start time")
	errNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "%s", "zero or negative query resolution step widths are not accepted. Try a positive integer")
	errStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "%s", "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")

	// Name of the cache control header.
	cacheControlHeader = "Cache-Control"
)

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

type prometheusCodec struct {
	tripperware.Codec
	sharded          bool
	compression      tripperware.Compression
	defaultCodecType tripperware.CodecType
}

func NewPrometheusCodec(sharded bool, compressionStr string, defaultCodecTypeStr string) *prometheusCodec { //nolint:revive
	compression := tripperware.NonCompression // default
	if compressionStr == string(tripperware.GzipCompression) {
		compression = tripperware.GzipCompression
	}

	defaultCodecType := tripperware.JsonCodecType // default
	if defaultCodecTypeStr == string(tripperware.ProtobufCodecType) {
		defaultCodecType = tripperware.ProtobufCodecType
	}

	return &prometheusCodec{
		sharded:          sharded,
		compression:      compression,
		defaultCodecType: defaultCodecType,
	}
}

func (c prometheusCodec) MergeResponse(ctx context.Context, req tripperware.Request, responses ...tripperware.Response) (tripperware.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "QueryRangeResponse.MergeResponse")
	sp.SetTag("response_count", len(responses))
	defer sp.Finish()
	if len(responses) == 0 {
		return tripperware.NewEmptyPrometheusResponse(false), nil
	}

	// Safety guard in case any response from results cache middleware
	// still uses the old queryrange.PrometheusResponse type.
	for i, resp := range responses {
		responses[i] = convertToTripperwarePrometheusResponse(resp)
	}
	return tripperware.MergeResponse(ctx, c.sharded, nil, responses...)
}

func (c prometheusCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (tripperware.Request, error) {
	result := tripperware.PrometheusRequest{Headers: map[string][]string{}}
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
				result.Headers[h] = hv
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
	promReq, ok := r.(*tripperware.PrometheusRequest)
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

	for n, hv := range promReq.Headers {
		for _, v := range hv {
			h.Add(n, v)
		}
	}

	tripperware.SetRequestHeaders(h, c.defaultCodecType, c.compression)

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
		return nil, httpgrpc.Errorf(r.StatusCode, "%s", string(buf))
	}
	log.LogFields(otlog.Int("bytes", len(buf)))

	var resp tripperware.PrometheusResponse
	err = tripperware.UnmarshalResponse(r, buf, &resp)

	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	// protobuf serialization treats empty slices as nil
	if resp.Data.ResultType == model.ValMatrix.String() && resp.Data.Result.GetMatrix().SampleStreams == nil {
		resp.Data.Result.GetMatrix().SampleStreams = []tripperware.SampleStream{}
	}

	if resp.Headers == nil {
		resp.Headers = []*tripperware.PrometheusResponseHeader{}
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &tripperware.PrometheusResponseHeader{Name: h, Values: hv})
	}
	return &resp, nil
}

func (prometheusCodec) EncodeResponse(ctx context.Context, _ *http.Request, res tripperware.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*tripperware.PrometheusResponse)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}

	if a != nil {
		m := a.Data.Result.GetMatrix()
		sp.LogFields(otlog.Int("series", len(m.GetSampleStreams())))

		queryStats := stats.FromContext(ctx)
		tripperware.SetQueryResponseStats(a, queryStats)
	}

	b, err := json.Marshal(a)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
	}

	sp.LogFields(otlog.Int("bytes", len(b)))

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{tripperware.ApplicationJson},
		},
		Body:          io.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
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
