//go:build requires_docker

package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Import cortexpb to register the cortexCodec (buffer pooling).
	_ "github.com/cortexproject/cortex/pkg/cortexpb"
)

// mockStoreGatewayServer implements storepb.StoreServer and streams
// pre-built SeriesResponse messages for benchmarking.
type mockStoreGatewayServer struct {
	storepb.UnimplementedStoreServer
	responses []*storepb.SeriesResponse
}

func (m *mockStoreGatewayServer) Series(_ *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	for _, resp := range m.responses {
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkGrpcStoreGatewayCalls benchmarks the full gRPC path for store gateway
// Series streaming with the cortexCodec and compression enabled.
// This is the store-gateway equivalent of BenchmarkGrpcCalls (which tests the ingester path).
//
// With SeriesResponse implementing ReleasableMessage, calling Free() after each Recv()
// returns the unmarshal buffer to the pool, reducing per-message allocations by ~32KB.
func BenchmarkGrpcStoreGatewayCalls(b *testing.B) {
	// Build realistic SeriesResponse messages (large enough to trigger buffer pooling).
	responses := make([]*storepb.SeriesResponse, 10)
	for i := range responses {
		responses[i] = createStoreGatewayBenchResponse(i)
	}

	mock := &mockStoreGatewayServer{responses: responses}

	// Start gRPC server.
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)

	gRPCServer := grpc.NewServer()
	storepb.RegisterStoreServer(gRPCServer, mock)

	go func() {
		if err := gRPCServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			b.Error(err)
		}
	}()
	defer gRPCServer.Stop()

	// Connect client with compression (zstd via cortexCodec default call options).
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(b, err)
	defer conn.Close()

	client := storepb.NewStoreClient(conn)

	// freeable checks if the response supports Free() (i.e., has MessageWithBufRef embedded).
	// This allows the benchmark to compile and run on both old builds (without Free)
	// and new builds (with Free), so you can compare results via benchstat.
	type freeable interface {
		Free()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := client.Series(context.Background(), &storepb.SeriesRequest{})
		require.NoError(b, err)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(b, err)
			if f, ok := interface{}(resp).(freeable); ok {
				f.Free()
			}
		}
	}
}

// createStoreGatewayBenchResponse creates a realistic SeriesResponse with chunk data
// large enough to exceed the buffer pooling threshold (~1KB).
func createStoreGatewayBenchResponse(n int) *storepb.SeriesResponse {
	lbls := labels.FromStrings(
		"__name__", fmt.Sprintf("http_requests_total_%d", n),
		"cluster", "us-east-1",
		"namespace", "production",
		"pod", fmt.Sprintf("web-server-deployment-7f8b9c6d4f-abc%02d", n),
		"container", "nginx",
		"instance", fmt.Sprintf("10.0.%d.%d:8080", n, n+1),
		"job", "kubernetes-pods",
	)

	// Create chunk data (~4KB per chunk, simulating real store gateway responses).
	chunkData := make([]byte, 4096)
	for i := range chunkData {
		chunkData[i] = byte((i + n) % 256)
	}

	numChunks := 5 + n
	chunks := make([]storepb.AggrChunk, numChunks)
	for i := 0; i < numChunks; i++ {
		chunks[i] = storepb.AggrChunk{
			MinTime: int64(i * 7200000),
			MaxTime: int64((i + 1) * 7200000),
			Raw: &storepb.Chunk{
				Type: storepb.Chunk_XOR,
				Data: chunkData,
			},
		}
	}

	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Series{
			Series: &storepb.Series{
				Labels: labelpb.ZLabelsFromPromLabels(lbls),
				Chunks: chunks,
			},
		},
	}
}
