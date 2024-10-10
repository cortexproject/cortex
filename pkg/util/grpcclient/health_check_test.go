package grpcclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	utillog "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
)

type healthClientMock struct {
	grpc_health_v1.HealthClient
	err  atomic.Error
	open atomic.Bool
}

func (h *healthClientMock) Close() error {
	h.open.Store(false)
	return nil
}

func (h *healthClientMock) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, h.err.Load()
}

func TestNewHealthCheckService(t *testing.T) {
	i := NewHealthCheckInterceptors(utillog.Logger)

	// set the gc timeout to 5 seconds
	i.instanceGcTimeout = time.Second * 5

	hMock := &healthClientMock{}
	i.healthClientFactory = func(cc *grpc.ClientConn) (grpc_health_v1.HealthClient, io.Closer) {
		hMock.open.Store(true)
		return hMock, hMock
	}

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	cfg := ConfigWithHealthCheck{
		HealthCheckConfig: HealthCheckConfig{
			UnhealthyThreshold: 2,
			Interval:           0,
			Timeout:            time.Second,
		},
	}

	client, err := grpc.NewClient("localhost:999", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	ui := i.UnaryHealthCheckInterceptor(&cfg)
	require.NoError(t, ui(context.Background(), "", struct{}{}, struct{}{}, client,
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			return nil
		}))

	instances := i.registeredInstances()
	require.Len(t, instances, 1)

	// Generate healthcheck error and wait instance to become unhealthy
	hMock.err.Store(errors.New("some error"))

	cortex_testutil.Poll(t, 5*time.Second, false, func() interface{} {
		return instances[0].isHealthy()
	})

	// Mark instance back to a healthy state
	hMock.err.Store(nil)
	cortex_testutil.Poll(t, 5*time.Second, true, func() interface{} {
		return instances[0].isHealthy()
	})

	cortex_testutil.Poll(t, i.instanceGcTimeout*2, 0, func() interface{} {
		return len(i.registeredInstances())
	})

	require.False(t, hMock.open.Load())
}

func TestNewHealthCheckInterceptors(t *testing.T) {
	i := NewHealthCheckInterceptors(utillog.Logger)
	hMock := &healthClientMock{}
	hMock.err.Store(fmt.Errorf("some error"))
	cfg := ConfigWithHealthCheck{
		HealthCheckConfig: HealthCheckConfig{
			UnhealthyThreshold: 2,
			Interval:           0,
			Timeout:            time.Second,
		},
	}
	i.healthClientFactory = func(cc *grpc.ClientConn) (grpc_health_v1.HealthClient, io.Closer) {
		hMock.open.Store(true)
		return hMock, hMock
	}

	ui := i.UnaryHealthCheckInterceptor(&cfg)
	ccUnhealthy, err := grpc.NewClient("localhost:999", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
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
	require.NoError(t, i.iteration(context.Background()))
	require.False(t, hMock.open.Load())

	//Should second call even with error
	require.NoError(t, ui(context.Background(), "", struct{}{}, struct{}{}, ccUnhealthy, invoker))

	require.Equal(t, invokedMap["localhost:999"], 2)

	// Second Healthcheck -> should mark as unhealthy
	require.NoError(t, i.iteration(context.Background()))
	require.False(t, hMock.open.Load())

	cortex_testutil.Poll(t, time.Second, true, func() interface{} {
		return errors.Is(ui(context.Background(), "", struct{}{}, struct{}{}, ccUnhealthy, invoker), unhealthyErr)
	})

	// Other instances should remain healthy
	require.NoError(t, ui(context.Background(), "", struct{}{}, struct{}{}, ccHealthy, invoker))

	// Should mark the instance back to healthy
	hMock.err.Store(nil)
	require.NoError(t, i.iteration(context.Background()))
	cortex_testutil.Poll(t, time.Second, true, func() interface{} {
		return ui(context.Background(), "", struct{}{}, struct{}{}, ccUnhealthy, invoker) == nil
	})
}
