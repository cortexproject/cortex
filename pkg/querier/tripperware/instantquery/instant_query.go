package instantquery

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
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

var (
	InstantQueryCodec tripperware.Codec = NewInstantQueryCodec("", "protobuf")

	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: false,
	}.Froze()
)

type instantQueryCodec struct {
	tripperware.Codec
	compression      tripperware.Compression
	defaultCodecType tripperware.CodecType
	now              func() time.Time
}

func NewInstantQueryCodec(compressionStr string, defaultCodecTypeStr string) instantQueryCodec {
	compression := tripperware.NonCompression // default
	if compressionStr == string(tripperware.GzipCompression) {
		compression = tripperware.GzipCompression
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
	log, ctx := spanlogger.New(ctx, "DecodeQueryInstantResponse") //nolint:ineffassign,staticcheck
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

	var resp tripperware.PrometheusResponse
	err = tripperware.UnmarshalResponse(r, buf, &resp)

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

func (instantQueryCodec) EncodeResponse(ctx context.Context, res tripperware.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*tripperware.PrometheusResponse)
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
			"Content-Type": []string{tripperware.ApplicationJson},
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

func decorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q; %v"
	if status, ok := status.FromError(err); ok {
		return httpgrpc.Errorf(int(status.Code()), errTmpl, field, status.Message())
	}
	return fmt.Errorf(errTmpl, field, err)
}
