package grpcclient

import (
	"context"
	"errors"
	"fmt"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	utillog "github.com/cortexproject/cortex/pkg/util/log"
)

type healthClientMock struct {
	grpc_health_v1.HealthClient
	err error
}

func (h *healthClientMock) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, h.err
}

func TestNewHealthCheckInterceptors(t *testing.T) {
	i := NewHealthCheckInterceptors(utillog.Logger)
	hMock := &healthClientMock{
		err: fmt.Errorf("some error"),
	}
	cfg := Config{
		HealthCheckConfig: HealthCheckConfig{
			UnhealthyThreshold: 2,
			Interval:           0,
			Timeout:            time.Second,
		},
	}
	i.healthClientFactory = func(cc grpc.ClientConnInterface) grpc_health_v1.HealthClient {
		return hMock
	}

	ui := i.UnaryHealthCheckInterceptor(cfg)
	ccUnhealthy, err := grpc.NewClient("localhost:999", grpc.WithTransportCredentials(insecure.NewCredentials()))
	ccHealthy, err := grpc.NewClient("localhost:111", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	invokedMap := map[string]int{}

	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		invokedMap[cc.Target()]++
		return nil
	}

	//Should allow first call
	require.NoError(t, ui(context.Background(), "", struct{}{}, struct{}{}, ccUnhealthy, invoker))

	// first health check
	i.iteration(context.Background())

	//Should second first call
	require.NoError(t, ui(context.Background(), "", struct{}{}, struct{}{}, ccUnhealthy, invoker))

	require.Equal(t, invokedMap["localhost:999"], 2)

	// Second Healthcheck -> should mark as unhealthy
	i.iteration(context.Background())

	cortex_testutil.Poll(t, time.Second, true, func() interface{} {
		return errors.Is(ui(context.Background(), "", struct{}{}, struct{}{}, ccUnhealthy, invoker), unhealthyErr)
	})

	// Other instances should remain healthy
	require.NoError(t, ui(context.Background(), "", struct{}{}, struct{}{}, ccHealthy, invoker))

	// Should mark the instance back to healthy
	hMock.err = nil
	i.iteration(context.Background())
	cortex_testutil.Poll(t, time.Second, true, func() interface{} {
		return ui(context.Background(), "", struct{}{}, struct{}{}, ccUnhealthy, invoker) == nil
	})

}
