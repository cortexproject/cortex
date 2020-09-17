package frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"go.uber.org/atomic"
	grpc "google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

type mockCloser struct{}

func (mockCloser) Close() error {
	return nil
}

type mockFrontendClient struct {
	failRecv bool
}

func (m *mockFrontendClient) Process(ctx context.Context, opts ...grpc.CallOption) (Frontend_ProcessClient, error) {
	return &mockFrontendProcessClient{
		ctx:      ctx,
		failRecv: m.failRecv,
	}, nil
}

type mockFrontendProcessClient struct {
	grpc.ClientStream

	ctx      context.Context
	failRecv bool
	wg       sync.WaitGroup
}

func (m *mockFrontendProcessClient) Send(*ClientToFrontend) error {
	m.wg.Done()
	return nil
}
func (m *mockFrontendProcessClient) Recv() (*FrontendToClient, error) {
	m.wg.Wait()
	m.wg.Add(1)

	if m.ctx.Err() != nil {
		return nil, m.ctx.Err()
	}

	if m.failRecv {
		return nil, errors.New("wups")
	}

	return &FrontendToClient{
		HttpRequest: &httpgrpc.HTTPRequest{},
	}, nil
}
func (m *mockFrontendProcessClient) Context() context.Context {
	return context.Background()
}

func TestConcurrency(t *testing.T) {
	calls := atomic.NewInt32(0)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Inc()
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
			mgr := newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), mockCloser{}, &mockFrontendClient{}, grpcclient.ConfigWithTLS{}, "querier")

			for _, c := range tt.concurrency {
				calls.Store(0)
				mgr.concurrentRequests(c)
				time.Sleep(50 * time.Millisecond)

				expected := int32(c)
				if expected < 0 {
					expected = 0
				}
				assert.Equal(t, expected, mgr.currentProcessors.Load())

				if expected > 0 {
					assert.Greater(t, calls.Load(), int32(0))
				}
			}

			mgr.stop()
			assert.Equal(t, int32(0), mgr.currentProcessors.Load())
		})
	}
}

func TestRecvFailDoesntCancelProcess(t *testing.T) {
	calls := atomic.NewInt32(0)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Inc()
		_, err := w.Write([]byte("Hello World"))
		assert.NoError(t, err)
	})

	client := &mockFrontendClient{
		failRecv: true,
	}

	mgr := newFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), mockCloser{}, client, grpcclient.ConfigWithTLS{}, "querier")

	mgr.concurrentRequests(1)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), mgr.currentProcessors.Load())

	mgr.stop()
	assert.Equal(t, int32(0), mgr.currentProcessors.Load())
	assert.Equal(t, int32(0), calls.Load())
}

func TestServeCancelStopsProcess(t *testing.T) {
	calls := atomic.NewInt32(0)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Inc()
		_, err := w.Write([]byte("Hello World"))
		assert.NoError(t, err)
	})

	client := &mockFrontendClient{
		failRecv: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgr := newFrontendManager(ctx, util.Logger, httpgrpc_server.NewServer(handler), mockCloser{}, client, grpcclient.ConfigWithTLS{GRPC: grpcclient.Config{MaxSendMsgSize: 100000}}, "querier")

	mgr.concurrentRequests(1)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), mgr.currentProcessors.Load())

	cancel()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(0), mgr.currentProcessors.Load())

	mgr.stop()
	assert.Equal(t, int32(0), mgr.currentProcessors.Load())
}
