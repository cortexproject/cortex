package client

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	// Register snappy compressor.
	_ "github.com/thanos-io/thanos/pkg/extgrpc/snappy"
)

type testHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (s *testHealthServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func TestSnappyGRPCCompression(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, &testHealthServer{})
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor("snappy")),
	)
	require.NoError(t, err)
	defer conn.Close()

	healthClient := grpc_health_v1.NewHealthClient(conn)
	_, err = healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
}
