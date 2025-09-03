package distributed_execution

import (
	"context"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/distributed_execution/querierpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

// TestQuerierPool verifies that the querier service client pool correctly manages
// client connections by testing address addition and client retrieval functionality
func TestQuerierPool(t *testing.T) {
	tests := []struct {
		name      string
		poolSetup func() (*client.Pool, *mockServer)
		test      func(*testing.T, *client.Pool, *mockServer)
	}{
		{
			name: "pool creates and manages clients",
			poolSetup: func() (*client.Pool, *mockServer) {

				mockServer := newMockServer(t)

				cfg := grpcclient.Config{
					MaxRecvMsgSize: 1024,
					MaxSendMsgSize: 1024,
				}

				reg := prometheus.NewRegistry()
				logger := log.NewNopLogger()

				pool := NewQuerierPool(cfg, reg, logger)

				return pool, mockServer
			},
			test: func(t *testing.T, pool *client.Pool, mockServer *mockServer) {
				// test getting client
				client, err := pool.GetClientFor(":8005")
				assert.NoError(t, err)
				assert.NotNil(t, client)

				// test client is reused
				client2, err := pool.GetClientFor(":8005")
				assert.NoError(t, err)
				assert.Equal(t, client, client2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, mockServer := tt.poolSetup()
			defer mockServer.Stop()
			tt.test(t, pool, mockServer)
		})
	}
}

type mockQuerierServer struct {
	querierpb.UnimplementedQuerierServer
}

func (m *mockQuerierServer) Next(req *querierpb.NextRequest, stream querierpb.Querier_NextServer) error {
	return nil
}

func (m *mockQuerierServer) Series(req *querierpb.SeriesRequest, stream querierpb.Querier_SeriesServer) error {
	return nil
}

type mockHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (m *mockHealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

type mockServer struct {
	server *grpc.Server
	addr   int
}

func newMockServer(t *testing.T) *mockServer {
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		LogSourceIPs:      true,
		MetricsNamespace:  "with_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	mockQuerier := &mockQuerierServer{}
	querierpb.RegisterQuerierServer(server.GRPC, mockQuerier)
	grpc_health_v1.RegisterHealthServer(server.GRPC, &mockHealthServer{})

	return &mockServer{
		server: server.GRPC,
		addr:   serverCfg.GRPCListenPort,
	}
}

func (m *mockServer) Stop() {
	if m.server != nil {
		m.server.Stop()
	}
}

// TestClientBuffer verifies that the streaming buffer matches the configured batch size and maintains correct data ordering
func TestClientBuffer(t *testing.T) {
	tests := []struct {
		name          string
		bufferData    []model.StepVector
		batchSize     int64
		numSteps      int
		expectedCalls [][]model.StepVector
		wantErr       bool
	}{
		{
			name:     "buffer with multiple batches",
			numSteps: 1,
			bufferData: []model.StepVector{
				{
					T:         1000,
					SampleIDs: []uint64{1},
					Samples:   []float64{10.0},
				},
				{
					T:         2000,
					SampleIDs: []uint64{1},
					Samples:   []float64{20.0},
				},
			},
			batchSize: 1,
			expectedCalls: [][]model.StepVector{
				{
					{
						T:         1000,
						SampleIDs: []uint64{1},
						Samples:   []float64{10.0},
					},
				},
				{
					{
						T:         2000,
						SampleIDs: []uint64{1},
						Samples:   []float64{20.0},
					},
				},
			},
			wantErr: false,
		},
		{
			name:     "single batch full buffer",
			numSteps: 1,
			bufferData: []model.StepVector{
				{
					T:         1000,
					SampleIDs: []uint64{1, 2},
					Samples:   []float64{10.0, 20.0},
				},
			},
			batchSize: 2,
			expectedCalls: [][]model.StepVector{
				{
					{
						T:         1000,
						SampleIDs: []uint64{1, 2},
						Samples:   []float64{10.0, 20.0},
					},
				},
			},
			wantErr: false,
		},
		{
			name:     "buffer with multiple batches",
			numSteps: 2,
			bufferData: []model.StepVector{
				{
					T:         1000,
					SampleIDs: []uint64{1},
					Samples:   []float64{10.0},
				},
				{
					T:         2000,
					SampleIDs: []uint64{1},
					Samples:   []float64{20.0},
				},
				{
					T:         3000,
					SampleIDs: []uint64{1},
					Samples:   []float64{30.0},
				},
				{
					T:         4000,
					SampleIDs: []uint64{1},
					Samples:   []float64{40.0},
				},
				{
					T:         5000,
					SampleIDs: []uint64{1},
					Samples:   []float64{50.0},
				},
			},
			batchSize: 2,
			expectedCalls: [][]model.StepVector{
				{
					{
						T:         1000,
						SampleIDs: []uint64{1},
						Samples:   []float64{10.0},
					},
					{
						T:         2000,
						SampleIDs: []uint64{1},
						Samples:   []float64{20.0},
					},
				},
				{
					{
						T:         3000,
						SampleIDs: []uint64{1},
						Samples:   []float64{30.0},
					},
					{
						T:         4000,
						SampleIDs: []uint64{1},
						Samples:   []float64{40.0},
					},
				},
				{
					{
						T:         5000,
						SampleIDs: []uint64{1},
						Samples:   []float64{50.0},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &DistributedRemoteExecution{
				mint:        0,
				maxt:        5000,
				step:        1000,
				currentStep: 0,
				numSteps:    tt.numSteps,
				batchSize:   tt.batchSize,
				buffer:      tt.bufferData,
				bufferIndex: 0,
				initialized: true,
			}

			mockClient := &mockQuerierClient{
				nextShouldBeCalled: false,
			}
			exec.client = mockClient

			ctx := context.Background()

			for i, expectedCall := range tt.expectedCalls { // next() server call
				result, err := exec.Next(ctx)
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				assert.Equal(t, expectedCall, result, "call %d", i)
			}
		})
	}
}

type mockQuerierClient struct {
	nextShouldBeCalled bool
}

func (m *mockQuerierClient) Series(ctx context.Context, req *querierpb.SeriesRequest, opts ...grpc.CallOption) (querierpb.Querier_SeriesClient, error) {
	return &mockSeriesStream{}, nil
}

func (m *mockQuerierClient) Next(ctx context.Context, req *querierpb.NextRequest, opts ...grpc.CallOption) (querierpb.Querier_NextClient, error) {
	return &mockNextStream{}, nil
}

type mockSeriesStream struct {
	querierpb.Querier_SeriesClient
}

func (m *mockSeriesStream) Recv() (*querierpb.SeriesBatch, error) {
	return nil, io.EOF
}

type mockNextStream struct {
	querierpb.Querier_NextClient
}

func (m *mockNextStream) Recv() (*querierpb.StepVectorBatch, error) {
	return nil, io.EOF
}
