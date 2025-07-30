package instantquery

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/munnerz/goautoneg"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/api/queryapi"
	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"

	"github.com/thanos-io/promql-engine/logicalplan"
)

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: false,
	}.Froze()

	rulerMIMEType = v1.MIMEType{Type: "application", SubType: tripperware.QueryResponseCortexMIMESubType}
	jsonMIMEType  = v1.MIMEType{Type: "application", SubType: "json"}
)

type instantQueryCodec struct {
	tripperware.Codec
	compression      tripperware.Compression
	defaultCodecType tripperware.CodecType
	now              func() time.Time
}

func NewInstantQueryCodec(compressionStr string, defaultCodecTypeStr string) instantQueryCodec {
	compression := tripperware.NonCompression // default
	switch compressionStr {
	case string(tripperware.GzipCompression):
		compression = tripperware.GzipCompression

	case string(tripperware.SnappyCompression):
		compression = tripperware.SnappyCompression

	case string(tripperware.ZstdCompression):
		compression = tripperware.ZstdCompression
	}

	defaultCodecType := tripperware.JsonCodecType // default
	if defaultCodecTypeStr == string(tripperware.ProtobufCodecType) {
		defaultCodecType = tripperware.ProtobufCodecType
	}

	return instantQueryCodec{
		compression:      compression,
		defaultCodecType: defaultCodecType,
		now:              time.Now,
	}
}

func (c instantQueryCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (tripperware.Request, error) {
	result := tripperware.PrometheusRequest{Headers: map[string][]string{}}
	var err error
	result.Time, err = util.ParseTimeParam(r, "time", c.now().Unix())
	if err != nil {
		return nil, queryapi.DecorateWithParamName(err, "time")
	}

	result.Query = r.FormValue("query")
	result.Stats = r.FormValue("stats")
	result.Path = r.URL.Path

	isSourceRuler := strings.Contains(r.Header.Get("User-Agent"), tripperware.RulerUserAgent)
	if isSourceRuler {
		// When the source is the Ruler, then forward whole headers
		result.Headers = r.Header
	} else {
		// Include the specified headers from http request in prometheusRequest.
		for _, header := range forwardHeaders {
			for h, hv := range r.Header {
				if strings.EqualFold(h, header) {
					result.Headers[h] = hv
					break
				}
			}
		}
	}

	return &result, nil
}

func (c instantQueryCodec) DecodeResponse(ctx context.Context, r *http.Response, _ tripperware.Request) (tripperware.Response, error) {
	log, ctx := spanlogger.New(ctx, "DecodeQueryInstantResponse") //nolint:ineffassign,staticcheck
	defer log.Finish()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	responseSizeHeader := r.Header.Get("X-Uncompressed-Length")
	responseSizeLimiter := limiter.ResponseSizeLimiterFromContextWithFallback(ctx)
	responseSize, hasSizeHeader, err := tripperware.ParseResponseSizeHeader(responseSizeHeader)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if hasSizeHeader {
		if err := responseSizeLimiter.AddResponseBytes(responseSize); err != nil {
			return nil, httpgrpc.Errorf(http.StatusUnprocessableEntity, "%s", err.Error())
		}
	}

	body, err := tripperware.BodyBytes(r, log)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	if !hasSizeHeader {
		if err := responseSizeLimiter.AddResponseBytes(len(body)); err != nil {
			return nil, httpgrpc.Errorf(http.StatusUnprocessableEntity, "%s", err.Error())
		}
	}

	if r.StatusCode/100 != 2 {
		return nil, httpgrpc.Errorf(r.StatusCode, "%s", string(body))
	}

	var resp tripperware.PrometheusResponse
	err = tripperware.UnmarshalResponse(r, body, &resp)

	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	// protobuf serialization treats empty slices as nil
	switch resp.Data.ResultType {
	case model.ValMatrix.String():
		if resp.Data.Result.GetMatrix().SampleStreams == nil {
			resp.Data.Result.GetMatrix().SampleStreams = []tripperware.SampleStream{}
		}
	case model.ValVector.String():
		if resp.Data.Result.GetVector().Samples == nil {
			resp.Data.Result.GetVector().Samples = []tripperware.Sample{}
		}
	}

	if resp.Headers == nil {
		resp.Headers = []*tripperware.PrometheusResponseHeader{}
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &tripperware.PrometheusResponseHeader{Name: h, Values: hv})
	}

	return &resp, nil
}

func (c instantQueryCodec) getSerializedBody(promReq *tripperware.PrometheusRequest) ([]byte, error) {
	var byteLP []byte
	var err error

	if promReq.LogicalPlan != nil {
		byteLP, err = logicalplan.Marshal(promReq.LogicalPlan.Root())
		if err != nil {
			return nil, err
		}
	}
	return byteLP, nil
}

func (c instantQueryCodec) EncodeRequest(ctx context.Context, r tripperware.Request) (*http.Request, error) {
	promReq, ok := r.(*tripperware.PrometheusRequest)
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

	h.Add("Content-Type", "application/x-www-form-urlencoded")

	isSourceRuler := strings.Contains(h.Get("User-Agent"), tripperware.RulerUserAgent)
	if !isSourceRuler {
		// When the source is the Ruler, skip set header
		tripperware.SetRequestHeaders(h, c.defaultCodecType, c.compression)
	}

	bodyBytes, err := c.getSerializedBody(promReq)
	if err != nil {
		return nil, err
	}
	form := url.Values{}
	form.Set("plan", string(bodyBytes))
	formEncoded := form.Encode()

	req := &http.Request{
		Method:     "POST",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       io.NopCloser(strings.NewReader(formEncoded)),
		Header:     h,
	}

	return req.WithContext(ctx), nil
}

func (c instantQueryCodec) EncodeResponse(ctx context.Context, req *http.Request, res tripperware.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*tripperware.PrometheusResponse)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}

	queryStats := stats.FromContext(ctx)
	tripperware.SetQueryResponseStats(a, queryStats)

	contentType, b, err := marshalResponse(a, req.Header.Get("Accept"))
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
	}

	sp.LogFields(otlog.Int("bytes", len(b)))

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{contentType},
		},
		Body:          io.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
}

func (instantQueryCodec) MergeResponse(ctx context.Context, req tripperware.Request, responses ...tripperware.Response) (tripperware.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "InstantQueryResponse.MergeResponse")
	sp.SetTag("response_count", len(responses))
	defer sp.Finish()

	if len(responses) == 0 {
		return tripperware.NewEmptyPrometheusResponse(true), nil
	}

	return tripperware.MergeResponse(ctx, true, req, responses...)
}

func marshalResponse(resp *tripperware.PrometheusResponse, acceptHeader string) (string, []byte, error) {
	for _, clause := range goautoneg.ParseAccept(acceptHeader) {
		if jsonMIMEType.Satisfies(clause) {
			b, err := json.Marshal(resp)
			return tripperware.ApplicationJson, b, err
		} else if rulerMIMEType.Satisfies(clause) {
			b, err := resp.Marshal()
			return tripperware.QueryResponseCortexMIMEType, b, err
		}
	}

	b, err := json.Marshal(resp)
	return tripperware.ApplicationJson, b, err
}
