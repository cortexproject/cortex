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
		concurrency []int
	}{
		{
			concurrency: []int{0},
		},
		{
			concurrency: []int{1},
		},
		{
			concurrency: []int{5},
		},
		{
			concurrency: []int{5, 3, 7},
		},
		{
			concurrency: []int{-1},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("Testing concurrency %v", tt.concurrency), func(t *testing.T) {
			mgr := newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), &mockFrontendClient{}, 0, 100000000)

			for _, c := range tt.concurrency {
				mgr.concurrentRequests(c)
				time.Sleep(50 * time.Millisecond)

				expected := int32(c)
				if expected < 0 {
					expected = 0
				}
				assert.Equal(t, expected, mgr.currentProcessors.Load())
			}

			mgr.stop()
			assert.Equal(t, int32(0), mgr.currentProcessors.Load())
		})
	}
}
