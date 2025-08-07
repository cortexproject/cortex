package ruler

import (
	"context"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

const (
	orgIDHeader      = "X-Scope-OrgID"
	instantQueryPath = "/api/v1/query"
	rangeQueryPath   = "/api/v1/query_range"

	mimeTypeForm = "application/x-www-form-urlencoded"
)

var jsonDecoder JsonDecoder
var protobufDecoder ProtobufDecoder

type FrontendClient struct {
	client               httpgrpc.HTTPClient
	timeout              time.Duration
	prometheusHTTPPrefix string
	queryResponseFormat  string
	decoders             map[string]Decoder
}

func NewFrontendClient(client httpgrpc.HTTPClient, timeout time.Duration, prometheusHTTPPrefix, queryResponseFormat string) *FrontendClient {
	return &FrontendClient{
		client:               client,
		timeout:              timeout,
		prometheusHTTPPrefix: prometheusHTTPPrefix,
		queryResponseFormat:  queryResponseFormat,
		decoders: map[string]Decoder{
			jsonDecoder.ContentType():     jsonDecoder,
			protobufDecoder.ContentType(): protobufDecoder,
		},
	}
}

func (p *FrontendClient) makeInstantRequest(ctx context.Context, qs string, ts time.Time, params map[string]string) (*httpgrpc.HTTPRequest, error) {
	args := make(url.Values)
	args.Set("query", qs)
	if !ts.IsZero() {
		args.Set("time", ts.Format(time.RFC3339Nano))
	}
	for k, v := range params {
		args.Set(k, v)
	}
	body := []byte(args.Encode())

	//lint:ignore faillint wrapper around upstream method
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	acceptHeader := ""
	switch p.queryResponseFormat {
	case queryResponseFormatJson:
		acceptHeader = jsonDecoder.ContentType()
	case queryResponseFormatProtobuf:
		acceptHeader = fmt.Sprintf("%s,%s", protobufDecoder.ContentType(), jsonDecoder.ContentType())
	}

	req := &httpgrpc.HTTPRequest{
		Method: http.MethodPost,
		Url:    p.prometheusHTTPPrefix + instantQueryPath,
		Body:   body,
		Headers: []*httpgrpc.Header{
			{Key: textproto.CanonicalMIMEHeaderKey("User-Agent"), Values: []string{fmt.Sprintf("%s/%s", tripperware.RulerUserAgent, version.Version)}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Type"), Values: []string{mimeTypeForm}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Length"), Values: []string{strconv.Itoa(len(body))}},
			{Key: textproto.CanonicalMIMEHeaderKey("Accept"), Values: []string{acceptHeader}},
			{Key: textproto.CanonicalMIMEHeaderKey(orgIDHeader), Values: []string{orgID}},
		},
	}

	return req, nil
}

func formatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
}

func (p *FrontendClient) makeRangeRequest(ctx context.Context, qs string, start, end time.Time, step float64) (*httpgrpc.HTTPRequest, error) {
	args := make(url.Values)
	args.Set("query", qs)
	args.Set("start", formatTime(start))
	args.Set("end", formatTime(end))
	args.Set("step", strconv.FormatFloat(step, 'f', -1, 64))
	body := []byte(args.Encode())

	//lint:ignore faillint wrapper around upstream method
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	acceptHeader := ""
	switch p.queryResponseFormat {
	case queryResponseFormatJson:
		acceptHeader = jsonDecoder.ContentType()
	case queryResponseFormatProtobuf:
		acceptHeader = fmt.Sprintf("%s,%s", protobufDecoder.ContentType(), jsonDecoder.ContentType())
	}

	req := &httpgrpc.HTTPRequest{
		Method: http.MethodPost,
		Url:    p.prometheusHTTPPrefix + rangeQueryPath,
		Body:   body,
		Headers: []*httpgrpc.Header{
			{Key: textproto.CanonicalMIMEHeaderKey("User-Agent"), Values: []string{fmt.Sprintf("Cortex/%s", version.Version)}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Type"), Values: []string{mimeTypeForm}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Length"), Values: []string{strconv.Itoa(len(body))}},
			{Key: textproto.CanonicalMIMEHeaderKey("Accept"), Values: []string{acceptHeader}},
			{Key: textproto.CanonicalMIMEHeaderKey(orgIDHeader), Values: []string{orgID}},
		},
	}

	return req, nil
}

func (p *FrontendClient) RangeQuery(ctx context.Context, qs string, from, to time.Time, step float64) (model.Matrix, error) {
	log, ctx := spanlogger.New(ctx, "FrontendClient.RangeQuery")
	defer log.Finish()

	req, err := p.makeRangeRequest(ctx, qs, from, to, step)
	if err != nil {
		level.Error(log).Log("err", err, "query", qs, "from", from, "to", to, "step", step)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	resp, err := p.client.Handle(ctx, req)
	if err != nil {
		level.Error(log).Log("err", err, "query", qs)
		return nil, err
	}

	contentType := extractHeader(resp.Headers, "Content-Type")
	decoder, ok := p.decoders[contentType]
	if !ok {
		err = fmt.Errorf("unknown content type: %s", contentType)
		level.Error(log).Log("err", err, "query", qs)
		return nil, err
	}

	m, warning, err := decoder.DecodeMatrix(resp.Body)
	if err != nil {
		level.Error(log).Log("err", err, "query", qs)
		return nil, err
	}

	if len(warning) > 0 {
		level.Warn(log).Log("warnings", warning, "query", qs)
	}

	return m, nil
}

func (p *FrontendClient) InstantQuery(ctx context.Context, qs string, t time.Time, params map[string]string) (promql.Vector, error) {
	log, ctx := spanlogger.New(ctx, "FrontendClient.InstantQuery")
	defer log.Finish()

	req, err := p.makeInstantRequest(ctx, qs, t, params)
	if err != nil {
		level.Error(log).Log("err", err, "query", qs)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	resp, err := p.client.Handle(ctx, req)

	if err != nil {
		level.Error(log).Log("err", err, "query", qs)
		return nil, err
	}

	contentType := extractHeader(resp.Headers, "Content-Type")
	decoder, ok := p.decoders[contentType]
	if !ok {
		err = fmt.Errorf("unknown content type: %s", contentType)
		level.Error(log).Log("err", err, "query", qs)
		return nil, err
	}

	vector, warning, err := decoder.DecodeVector(resp.Body)
	if err != nil {
		level.Error(log).Log("err", err, "query", qs)
		return nil, err
	}

	if len(warning) > 0 {
		level.Warn(log).Log("warnings", warning, "query", qs)
	}

	return vector, nil
}

func extractHeader(headers []*httpgrpc.Header, target string) string {
	for _, h := range headers {
		if h.Key == target && len(h.Values) > 0 {
			return h.Values[0]
		}
	}

	return ""
}
