package distributor

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/ingester/client"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type server struct {
	lis        net.Listener
	port       string
	startedErr chan error // sent nil or an error after server starts
	grpcServer *grpc.Server
}

// start starts server. Other goroutines should block on s.startedErr for further operations.
func (s *server) start(t testing.TB, ingester client.IngesterServer) {
	var err error
	s.lis, err = net.Listen("tcp", "localhost:0")
	if err != nil {
		s.startedErr <- fmt.Errorf("failed to listen: %v", err)
		return
	}
	_, p, err := net.SplitHostPort(s.lis.Addr().String())
	if err != nil {
		s.startedErr <- fmt.Errorf("failed to parse listener address: %v", err)
		return
	}
	s.port = p

	s.grpcServer = grpc.NewServer(grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	client.RegisterIngesterServer(s.grpcServer, ingester)
	s.startedErr <- nil
	go s.grpcServer.Serve(s.lis)
}

func (s *server) wait(t testing.TB, timeout time.Duration) {
	select {
	case err := <-s.startedErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(timeout):
		t.Fatalf("Timed out after %v waiting for server to be ready", timeout)
	}
}

func (s *server) stop() {
	s.grpcServer.Stop()
}

func setUpMockGRPCServer(t testing.TB) (*server, client.IngesterClient) {
	server := &server{startedErr: make(chan error, 1)}
	ingester := &mockIngesterServer{}
	go server.start(t, ingester)
	server.wait(t, 2*time.Second)
	addr := "localhost:" + server.port

	cc, err := client.MakeIngesterClient(addr, 10*time.Second, true)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return server, cc
}

type mockIngesterServer struct {
}

func (i mockIngesterServer) Push(ctx context.Context, in *client.WriteRequest) (*client.WriteResponse, error) {
	return &client.WriteResponse{}, nil
}

func (i mockIngesterServer) Query(ctx context.Context, in *client.QueryRequest) (*client.QueryResponse, error) {
	return &client.QueryResponse{}, nil
}

func (i mockIngesterServer) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	return nil, nil
}

func (i mockIngesterServer) UserStats(ctx context.Context, in *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	return nil, nil
}

func (i mockIngesterServer) MetricsForLabelMatchers(ctx context.Context, in *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	return nil, nil
}

func (i mockIngesterServer) TransferChunks(client.Ingester_TransferChunksServer) error {
	return nil
}

func TestInvoke(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	server, cc := setUpMockGRPCServer(t)
	defer cc.(io.Closer).Close()
	defer server.stop()

	request := createRequest(10)
	_, err := cc.Push(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDistributorPush2(b *testing.B) {
	ctx := user.InjectOrgID(context.Background(), "user")
	server, cc := setUpMockGRPCServer(b)
	defer cc.(io.Closer).Close()
	defer server.stop()

	request := createRequest(10)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := cc.Push(ctx, request)
		if err != nil {
			b.Fatal(err)
		}
	}
}
