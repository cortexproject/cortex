package frontend

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/kit/log"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	jaeger "github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/user"
)

func TestFrontend(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello World"))
	})
	test := func(addr string) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/", addr), nil)
		require.NoError(t, err)
		err = user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(context.Background(), "1"), req)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)

		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, "Hello World", string(body))
	}
	testFrontend(t, handler, test)
}

func TestFrontendRetries(t *testing.T) {
	try := int32(0)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&try, 1) == 5 {
			w.Write([]byte("Hello World"))
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
	})
	test := func(addr string) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/", addr), nil)
		require.NoError(t, err)
		err = user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(context.Background(), "1"), req)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)

		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, "Hello World", string(body))
	}
	testFrontend(t, handler, test)
}

func TestFrontendPropagateTrace(t *testing.T) {
	closer, err := config.Configuration{}.InitGlobalTracer("test")
	require.NoError(t, err)
	defer closer.Close()

	observedTraceID := make(chan string, 2)

	handler := middleware.Tracer{}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sp := opentracing.SpanFromContext(r.Context())
		defer sp.Finish()

		traceID := fmt.Sprintf("%v", sp.Context().(jaeger.SpanContext).TraceID())
		observedTraceID <- traceID

		w.Write([]byte(responseBody))
	}))

	test := func(addr string) {
		sp, ctx := opentracing.StartSpanFromContext(context.Background(), "client")
		defer sp.Finish()
		traceID := fmt.Sprintf("%v", sp.Context().(jaeger.SpanContext).TraceID())

		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/%s", addr, query), nil)
		require.NoError(t, err)
		req = req.WithContext(ctx)
		err = user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(ctx, "1"), req)
		require.NoError(t, err)

		req, tr := nethttp.TraceRequest(opentracing.GlobalTracer(), req)
		defer tr.Finish()

		client := http.Client{
			Transport: &nethttp.Transport{},
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)

		defer resp.Body.Close()
		_, err = ioutil.ReadAll(resp.Body)
		require.NoError(t, err)

		// Query should do two calls.
		assert.Equal(t, traceID, <-observedTraceID)
		assert.Equal(t, traceID, <-observedTraceID)
	}
	testFrontend(t, handler, test)
}

func testFrontend(t *testing.T, handler http.Handler, test func(addr string)) {
	logger := log.NewNopLogger() //log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	var (
		config       Config
		workerConfig WorkerConfig
	)
	flagext.DefaultValues(&config, &workerConfig)
	config.SplitQueriesByDay = true

	// localhost:0 prevents firewall warnings on Mac OS X.
	grpcListen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	workerConfig.Address = grpcListen.Addr().String()

	httpListen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	frontend, err := New(config, logger)
	require.NoError(t, err)
	defer frontend.Close()

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(otgrpc.OpenTracingStreamServerInterceptor(opentracing.GlobalTracer())),
	)
	defer grpcServer.GracefulStop()

	RegisterFrontendServer(grpcServer, frontend)

	httpServer := http.Server{
		Handler: middleware.Merge(
			middleware.AuthenticateUser,
			middleware.Tracer{},
		).Wrap(frontend),
	}
	defer httpServer.Shutdown(context.Background())

	go httpServer.Serve(httpListen)
	go grpcServer.Serve(grpcListen)

	worker, err := NewWorker(workerConfig, httpgrpc_server.NewServer(handler), logger)
	require.NoError(t, err)
	defer worker.Stop()

	test(httpListen.Addr().String())
}
