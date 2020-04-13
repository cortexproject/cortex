package frontend

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	grpc "google.golang.org/grpc"
)

type mockFrontendClient struct {
}

func (m *mockFrontendClient) Process(ctx context.Context, opts ...grpc.CallOption) (Frontend_ProcessClient, error) {
	return &mockFrontendProcessClient{}, nil
}

type mockFrontendProcessClient struct {
	grpc.ClientStream

	wg sync.WaitGroup
}

func (m *mockFrontendProcessClient) Send(*ProcessResponse) error {
	m.wg.Done()
	return nil
}
func (m *mockFrontendProcessClient) Recv() (*ProcessRequest, error) {
	m.wg.Wait()
	m.wg.Add(1)

	return &ProcessRequest{
		HttpRequest: &httpgrpc.HTTPRequest{},
	}, nil
}
func (m *mockFrontendProcessClient) Context() context.Context {
	return context.Background()
}

func TestConstructionAndStop(t *testing.T) {
	var calls int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("Hello World"))
		atomic.AddInt32(&calls, 1)
		assert.NoError(t, err)
	})

	mgr := newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), &mockFrontendClient{}, 0, 100000000)
	mgr.stop()

	assert.Equal(t, int32(0), calls)
}

func TestSingleConcurrency(t *testing.T) {
	concurrency := 1

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("Hello World"))
		assert.NoError(t, err)
	})

	mgr := newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), &mockFrontendClient{}, 0, 100000000)
	mgr.concurrentRequests(concurrency)
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(concurrency), mgr.currentProcessors.Load())
	mgr.stop()
	assert.Equal(t, int32(0), mgr.currentProcessors.Load())
}
