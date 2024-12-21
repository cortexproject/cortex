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
	mimeTypeForm     = "application/x-www-form-urlencoded"
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

func (p *FrontendClient) makeRequest(ctx context.Context, qs string, ts time.Time) (*httpgrpc.HTTPRequest, error) {
	args := make(url.Values)
	args.Set("query", qs)
	if !ts.IsZero() {
		args.Set("time", ts.Format(time.RFC3339Nano))
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

func (p *FrontendClient) InstantQuery(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
	log, ctx := spanlogger.New(ctx, "FrontendClient.InstantQuery")
	defer log.Span.Finish()

	req, err := p.makeRequest(ctx, qs, t)
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

	vector, warning, err := decoder.Decode(resp.Body)
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
