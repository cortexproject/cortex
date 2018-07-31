package frontend

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/util"
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

func testFrontend(t *testing.T, handler http.Handler, test func(addr string)) {
	logger := log.NewNopLogger() //log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	var (
		config       Config
		workerConfig WorkerConfig
	)
	util.DefaultValues(&config, &workerConfig)

	grpcListen, err := net.Listen("tcp", "")
	require.NoError(t, err)
	workerConfig.Address = grpcListen.Addr().String()

	httpListen, err := net.Listen("tcp", "")
	require.NoError(t, err)

	frontend, err := New(config, logger)
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	defer grpcServer.GracefulStop()

	RegisterFrontendServer(grpcServer, frontend)

	httpServer := http.Server{
		Handler: middleware.AuthenticateUser.Wrap(frontend),
	}
	defer httpServer.Shutdown(context.Background())

	go httpServer.Serve(httpListen)
	go grpcServer.Serve(grpcListen)

	worker, err := NewWorker(workerConfig, httpgrpc_server.NewServer(handler), logger)
	require.NoError(t, err)
	defer worker.Stop()

	test(httpListen.Addr().String())
}
