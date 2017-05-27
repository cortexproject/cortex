package httpgrpc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"github.com/sercand/kuberesolver"
	"golang.org/x/net/context"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

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
	if recorder.Code/100 == 5 {
		return nil, errorFromHTTPResponse(resp)
	}
	return resp, err
}

// Client is a http.Handler that forwards the request over gRPC.
type Client struct {
	mtx       sync.RWMutex
	service   string
	namespace string
	port      string
	client    HTTPClient
	conn      *grpc.ClientConn
}

// ParseURL deals with direct:// style URLs, as well as kubernetes:// urls.
// For backwards compatibility it treats URLs without schems as kubernetes://.
func ParseURL(unparsed string) (string, []grpc.DialOption, error) {
	parsed, err := url.Parse(unparsed)
	if err != nil {
		return "", nil, err
	}

	scheme, host := parsed.Scheme, parsed.Host
	if !strings.Contains(unparsed, "://") {
		scheme, host = "kubernetes", unparsed
	}

	switch scheme {
	case "direct":
		return host, nil, err

	case "kubernetes":
		host, port, err := net.SplitHostPort(host)
		if err != nil {
			return "", nil, err
		}
		parts := strings.SplitN(host, ".", 2)
		service, namespace := parts[0], "default"
		if len(parts) == 2 {
			namespace = parts[1]
		}
		balancer := kuberesolver.NewWithNamespace(namespace)
		address := fmt.Sprintf("kubernetes://%s:%s", service, port)
		dialOptions := []grpc.DialOption{balancer.DialOption()}
		return address, dialOptions, nil

	default:
		return "", nil, fmt.Errorf("unrecognised scheme: %s", parsed.Scheme)
	}
}

// NewClient makes a new Client, given a kubernetes service address.
func NewClient(address string) (*Client, error) {
	address, dialOptions, err := ParseURL(address)
	if err != nil {
		return nil, err
	}

	dialOptions = append(
		dialOptions,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
	)

	conn, err := grpc.Dial(address, dialOptions...)
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
		// Some errors will actually contain a valid resp, just need to unpack it
		var ok bool
		resp, ok = httpResponseFromError(err)

		if !ok {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	toHeader(resp.Headers, w.Header())
	w.WriteHeader(int(resp.Code))
	if _, err := w.Write(resp.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func errorFromHTTPResponse(resp *HTTPResponse) error {
	a, err := ptypes.MarshalAny(resp)
	if err != nil {
		return err
	}

	return status.ErrorProto(&spb.Status{
		Code:    resp.Code,
		Message: string(resp.Body),
		Details: []*any.Any{a},
	})
}

func httpResponseFromError(err error) (*HTTPResponse, bool) {
	s, ok := status.FromError(err)
	if !ok {
		fmt.Println("not status")
		return nil, false
	}

	status := s.Proto()
	if len(status.Details) != 1 {
		return nil, false
	}

	var resp HTTPResponse
	if err := ptypes.UnmarshalAny(status.Details[0], &resp); err != nil {
		log.Errorf("Got error containing non-response: %v", err)
		return nil, false
	}

	return &resp, true
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
