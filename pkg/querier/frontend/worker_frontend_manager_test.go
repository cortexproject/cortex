package frontend

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/stretchr/testify/assert"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	grpc "google.golang.org/grpc"
)

type mockFrontendClient struct {
}

func (m *mockFrontendClient) Process(ctx context.Context, opts ...grpc.CallOption) (Frontend_ProcessClient, error) {
	return nil, nil
}

func TestConstructionAndStop(t *testing.T) {
	var calls int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("Hello World"))
		atomic.AddInt32(&calls, 1)
		assert.NoError(t, err)
	})

	mgr := NewFrontendManager(context.Background(), util.Logger, httpgrpc_server.NewServer(handler), &mockFrontendClient{}, 0, 100000000)
	mgr.stop()

	assert.Equal(t, int32(0), calls)
}
