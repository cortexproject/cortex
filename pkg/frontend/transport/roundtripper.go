package transport

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
)

// GrpcRoundTripper is similar to http.RoundTripper, but works with HTTP requests converted to protobuf messages.
type GrpcRoundTripper interface {
	RoundTripGRPC(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error)
}

func AdaptGrpcRoundTripperToHTTPRoundTripper(r GrpcRoundTripper, retry *Retry) http.RoundTripper {
	return &grpcRoundTripperAdapter{roundTripper: r, retry: retry}
}

// This adapter wraps GrpcRoundTripper and converted it into http.RoundTripper
type grpcRoundTripperAdapter struct {
	roundTripper GrpcRoundTripper
	retry        *Retry
}

type buffer struct {
	buff []byte
	io.ReadCloser
}

func (b *buffer) Bytes() []byte {
	return b.buff
}

func (a *grpcRoundTripperAdapter) RoundTrip(r *http.Request) (*http.Response, error) {
	req, err := server.HTTPRequest(r)
	if err != nil {
		return nil, err
	}

	return a.retry.Do(r.Context(), func() (*http.Response, error) {
		resp, err := a.roundTripper.RoundTripGRPC(r.Context(), req)
		if err != nil {
			return nil, err
		}
		return httpGRPCRespToHTTPResp(resp), nil
	})
}

func httpGRPCRespToHTTPResp(resp *httpgrpc.HTTPResponse) *http.Response {
	httpResp := &http.Response{
		StatusCode:    int(resp.Code),
		Body:          &buffer{buff: resp.Body, ReadCloser: io.NopCloser(bytes.NewReader(resp.Body))},
		Header:        http.Header{},
		ContentLength: int64(len(resp.Body)),
	}
	for _, h := range resp.Headers {
		httpResp.Header[h.Key] = h.Values
	}
	return httpResp
}
