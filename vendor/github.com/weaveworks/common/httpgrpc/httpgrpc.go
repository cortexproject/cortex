package httpgrpc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"github.com/sercand/kuberesolver"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
)

// Server implements HTTPServer.  HTTPServer is a generated interface that gRPC
// servers must implement.
type Server struct {
	handler http.Handler
}

// NewServer makes a new Server.
func NewServer(handler http.Handler) *Server {
	return &Server{
		handler: handler,
	}
}

// Handle implements HTTPServer.
func (s Server) Handle(ctx context.Context, r *HTTPRequest) (*HTTPResponse, error) {
	req, err := http.NewRequest(r.Method, r.Url, ioutil.NopCloser(bytes.NewReader(r.Body)))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	toHeader(r.Headers, req.Header)
	req.RequestURI = r.Url
	recorder := httptest.NewRecorder()
	s.handler.ServeHTTP(recorder, req)
	resp := &HTTPResponse{
		Code:    int32(recorder.Code),
		Headers: fromHeader(recorder.Header()),
		Body:    recorder.Body.Bytes(),
	}
	return resp, nil
}

// Client is a http.Handler that forwards the request over gRPC.
type Client struct {
	client HTTPClient
	conn   *grpc.ClientConn
}

// NewClient makes a new Client, given a kubernetes service address.  Expects
// an address of the form <service>.<namespace>:<port>
func NewClient(address string) (*Client, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	parts := strings.SplitN(host, ".", 2)
	service, namespace := parts[0], "default"
	if len(parts) == 2 {
		namespace = parts[1]
	}
	balancer := kuberesolver.NewWithNamespace(namespace)
	conn, err := grpc.Dial(
		fmt.Sprintf("kubernetes://%s:%s", service, port),
		balancer.DialOption(),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		client: NewHTTPClient(conn),
		conn:   conn,
	}, nil
}

// ServeHTTP implements http.Handler
func (c *Client) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req := &HTTPRequest{
		Method:  r.Method,
		Url:     r.RequestURI,
		Body:    body,
		Headers: fromHeader(r.Header),
	}

	resp, err := c.client.Handle(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	toHeader(resp.Headers, w.Header())
	w.WriteHeader(int(resp.Code))
	if _, err := w.Write(resp.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func toHeader(hs []*Header, header http.Header) {
	for _, h := range hs {
		header[h.Key] = h.Values
	}
}

func fromHeader(hs http.Header) []*Header {
	result := make([]*Header, 0, len(hs))
	for k, vs := range hs {
		result = append(result, &Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}
