package cortex

import (
	"bytes"
	"context"
	"flag"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/local"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/frontend/v1/frontendv1pb"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/scheduler/schedulerpb"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestCortex(t *testing.T) {
	cfg := Config{
		// Include the network names here explicitly. When CLI flags are registered
		// these values are set as defaults but since we aren't registering them, we
		// need to include the defaults here. These were hardcoded in a previous version
		// of weaveworks server.
		Server: server.Config{
			GRPCListenNetwork: server.DefaultNetwork,
			HTTPListenNetwork: server.DefaultNetwork,
		},
		Storage: storage.Config{
			Engine: storage.StorageEngineBlocks, // makes config easier
		},
		Ingester: ingester.Config{
			BlocksStorageConfig: tsdb.BlocksStorageConfig{
				Bucket: bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						Endpoint: "localhost",
					},
				},
			},
			LifecyclerConfig: ring.LifecyclerConfig{
				RingConfig: ring.Config{
					KVStore: kv.Config{
						Store: "inmemory",
					},
					ReplicationFactor: 3,
				},
				InfNames: []string{"en0", "eth0", "lo0", "lo"},
			},
		},
		BlocksStorage: tsdb.BlocksStorageConfig{
			Bucket: bucket.Config{
				Backend: bucket.S3,
				S3: s3.Config{
					Endpoint: "localhost",
				},
			},
			BucketStore: tsdb.BucketStoreConfig{
				ChunkPoolMinBucketSizeBytes: tsdb.ChunkPoolDefaultMinBucketSize,
				ChunkPoolMaxBucketSizeBytes: tsdb.ChunkPoolDefaultMaxBucketSize,
				IndexCache: tsdb.IndexCacheConfig{
					Backend: tsdb.IndexCacheBackendInMemory,
				},
			},
		},
		RulerStorage: rulestore.Config{
			Config: bucket.Config{
				Backend: "local",
			},
			Local: local.Config{
				Directory: os.TempDir(),
			},
		},

		Target: []string{All, Compactor},
	}

	c, err := New(cfg)
	require.NoError(t, err)

	serviceMap, err := c.ModuleManager.InitModuleServices(cfg.Target...)
	require.NoError(t, err)
	require.NotNil(t, serviceMap)

	for m, s := range serviceMap {
		// make sure each service is still New
		require.Equal(t, services.New, s.State(), "module: %s", m)
	}

	// check random modules that we expect to be configured when using Target=All
	require.NotNil(t, serviceMap[Server])
	require.NotNil(t, serviceMap[IngesterService])
	require.NotNil(t, serviceMap[Ring])
	require.NotNil(t, serviceMap[DistributorService])

	// check that compactor is configured which is not part of Target=All
	require.NotNil(t, serviceMap[Compactor])
}

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		getTestConfig func() *Config
		expectedError error
	}{
		{
			name: "should pass validation if the http prefix is empty",
			getTestConfig: func() *Config {
				return newDefaultConfig()
			},
			expectedError: nil,
		},
		{
			name: "should pass validation if the http prefix starts with /",
			getTestConfig: func() *Config {
				configuration := newDefaultConfig()
				configuration.HTTPPrefix = "/test"
				return configuration
			},
			expectedError: nil,
		},
		{
			name: "should fail validation for invalid prefix",
			getTestConfig: func() *Config {
				configuration := newDefaultConfig()
				configuration.HTTPPrefix = "test"
				return configuration
			},
			expectedError: errInvalidHTTPPrefix,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.getTestConfig().Validate(nil)
			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGrpcAuthMiddleware(t *testing.T) {
	prepareGlobalMetricsRegistry(t)

	cfg := Config{
		AuthEnabled: true, // We must enable this to enable Auth middleware for gRPC server.
		Server:      getServerConfig(t),
		Target:      []string{API}, // Something innocent that doesn't require much config.
	}

	msch := &mockGrpcServiceHandler{}
	ctx := context.Background()

	// Setup server, using Cortex config. This includes authentication middleware.
	{
		c, err := New(cfg)
		require.NoError(t, err)

		serv, err := c.initServer()
		require.NoError(t, err)

		schedulerpb.RegisterSchedulerForQuerierServer(c.Server.GRPC, msch)
		frontendv1pb.RegisterFrontendServer(c.Server.GRPC, msch)

		require.NoError(t, services.StartAndAwaitRunning(ctx, serv))
		defer func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, serv))
		}()
	}

	conn, err := grpc.Dial(net.JoinHostPort(cfg.Server.GRPCListenAddress, strconv.Itoa(cfg.Server.GRPCListenPort)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	{
		// Verify that we can call frontendClient.NotifyClientShutdown without user in the context, and we don't get any error.
		require.False(t, msch.clientShutdownCalled.Load())
		frontendClient := frontendv1pb.NewFrontendClient(conn)
		_, err = frontendClient.NotifyClientShutdown(ctx, &frontendv1pb.NotifyClientShutdownRequest{ClientID: "random-client-id"})
		require.NoError(t, err)
		require.True(t, msch.clientShutdownCalled.Load())
	}

	{
		// Verify that we can call schedulerClient.NotifyQuerierShutdown without user in the context, and we don't get any error.
		require.False(t, msch.querierShutdownCalled.Load())
		schedulerClient := schedulerpb.NewSchedulerForQuerierClient(conn)
		_, err = schedulerClient.NotifyQuerierShutdown(ctx, &schedulerpb.NotifyQuerierShutdownRequest{QuerierID: "random-querier-id"})
		require.NoError(t, err)
		require.True(t, msch.querierShutdownCalled.Load())
	}
}

func TestFlagDefaults(t *testing.T) {
	c := Config{}

	f := flag.NewFlagSet("test", flag.PanicOnError)
	c.RegisterFlags(f)

	buf := bytes.Buffer{}

	f.SetOutput(&buf)
	f.PrintDefaults()

	const delim = '\n'

	minTimeChecked := false
	pingWithoutStreamChecked := false
	for {
		line, err := buf.ReadString(delim)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		if strings.Contains(line, "-server.grpc.keepalive.min-time-between-pings") {
			nextLine, err := buf.ReadString(delim)
			require.NoError(t, err)
			assert.Contains(t, nextLine, "(default 10s)")
			minTimeChecked = true
		}

		if strings.Contains(line, "-server.grpc.keepalive.ping-without-stream-allowed") {
			nextLine, err := buf.ReadString(delim)
			require.NoError(t, err)
			assert.Contains(t, nextLine, "(default true)")
			pingWithoutStreamChecked = true
		}
	}

	require.True(t, minTimeChecked)
	require.True(t, pingWithoutStreamChecked)

	require.Equal(t, true, c.Server.GRPCServerPingWithoutStreamAllowed)
	require.Equal(t, 10*time.Second, c.Server.GRPCServerMinTimeBetweenPings)
}

// Generates server config, with gRPC listening on random port.
func getServerConfig(t *testing.T) server.Config {
	listen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	host, port, err := net.SplitHostPort(listen.Addr().String())
	require.NoError(t, err)
	require.NoError(t, listen.Close())

	portNum, err := strconv.Atoi(port)
	require.NoError(t, err)

	return server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		GRPCListenNetwork: server.DefaultNetwork,

		GRPCListenAddress: host,
		GRPCListenPort:    portNum,

		GPRCServerMaxRecvMsgSize: 1024,
	}
}

type mockGrpcServiceHandler struct {
	clientShutdownCalled  atomic.Bool
	querierShutdownCalled atomic.Bool
}

func (m *mockGrpcServiceHandler) NotifyClientShutdown(_ context.Context, _ *frontendv1pb.NotifyClientShutdownRequest) (*frontendv1pb.NotifyClientShutdownResponse, error) {
	m.clientShutdownCalled.Store(true)
	return &frontendv1pb.NotifyClientShutdownResponse{}, nil
}

func (m *mockGrpcServiceHandler) NotifyQuerierShutdown(_ context.Context, _ *schedulerpb.NotifyQuerierShutdownRequest) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	m.querierShutdownCalled.Store(true)
	return &schedulerpb.NotifyQuerierShutdownResponse{}, nil
}

func (m *mockGrpcServiceHandler) Process(_ frontendv1pb.Frontend_ProcessServer) error {
	panic("implement me")
}

func (m *mockGrpcServiceHandler) QuerierLoop(_ schedulerpb.SchedulerForQuerier_QuerierLoopServer) error {
	panic("implement me")
}

func prepareGlobalMetricsRegistry(t *testing.T) {
	oldReg, oldGat := prometheus.DefaultRegisterer, prometheus.DefaultGatherer

	reg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer, prometheus.DefaultGatherer = reg, reg

	t.Cleanup(func() {
		prometheus.DefaultRegisterer, prometheus.DefaultGatherer = oldReg, oldGat
	})
}
