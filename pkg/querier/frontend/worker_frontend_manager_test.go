package frontend

import (
	"context"
	"fmt"
	"net/http"
	"sync"
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

func TestConcurrency(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("Hello World"))
		assert.NoError(t, err)
	})

	tests := []struct {
		concurrency int
	}{
		{
			concurrency: 0,
		},
		{
			concurrency: 1,
		},
		{
			concurrency: 5,
		},
		{
			concurrency: 30,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("Testing concurrency %d", tt.concurrency), func(t *testing.T) {
			mgr := newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), &mockFrontendClient{}, 0, 100000000)
			mgr.concurrentRequests(tt.concurrency)
			time.Sleep(100 * time.Millisecond)

			assert.Equal(t, int32(tt.concurrency), mgr.currentProcessors.Load())
			mgr.stop()
			assert.Equal(t, int32(0), mgr.currentProcessors.Load())
		})
	}
}
