package cortex

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestServerStopViaContext(t *testing.T) {
	// server registers some metrics to default registry
	savedRegistry := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	defer func() {
		prometheus.DefaultRegisterer = savedRegistry
	}()

	serv, err := server.New(server.Config{HTTPListenNetwork: server.DefaultNetwork})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	s := NewServerService(serv, func() []services.Service { return nil })
	require.NoError(t, s.StartAsync(ctx))

	// should terminate soon, since context has short timeout
	require.NoError(t, s.AwaitTerminated(context.Background()))
}

func TestServerStopViaShutdown(t *testing.T) {
	// server registers some metrics to default registry
	savedRegistry := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	defer func() {
		prometheus.DefaultRegisterer = savedRegistry
	}()

	serv, err := server.New(server.Config{HTTPListenNetwork: server.DefaultNetwork})
	require.NoError(t, err)

	s := NewServerService(serv, func() []services.Service { return nil })
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))

	// Shutting down HTTP/gRPC servers makes Server stop, but ServerService doesn't expect that to happen.
	serv.Shutdown()

	require.Error(t, s.AwaitTerminated(context.Background()))
	require.Equal(t, services.Failed, s.State())
}

func TestServerStopViaStop(t *testing.T) {
	// server registers some metrics to default registry
	savedRegistry := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	defer func() {
		prometheus.DefaultRegisterer = savedRegistry
	}()

	serv, err := server.New(server.Config{HTTPListenNetwork: server.DefaultNetwork})
	require.NoError(t, err)

	s := NewServerService(serv, func() []services.Service { return nil })
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))

	serv.Stop()

	// Stop makes Server stop, but ServerService doesn't expect that to happen.
	require.Error(t, s.AwaitTerminated(context.Background()))
	require.Equal(t, services.Failed, s.State())
}
