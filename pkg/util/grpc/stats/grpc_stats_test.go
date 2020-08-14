package stats

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestGrpcStats(t *testing.T) {
	reg := prometheus.NewRegistry()
	stats := NewStatsHandler(reg)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StatsHandler(stats), grpc.MaxRecvMsgSize(10e6))
	defer serv.GracefulStop()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(serv, hs)

	closed := false
	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	defer func() {
		if !closed {
			require.NoError(t, conn.Close())
		}
	}()

	hc := grpc_health_v1.NewHealthClient(conn)

	// First request (empty).
	resp, err := hc.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)

	// Second request, with large service name. This returns error, which doesn't count as "payload".
	_, err = hc.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: generateString(t, 8*1024*1024),
	})
	require.EqualError(t, err, "rpc error: code = NotFound desc = unknown service")

	err = testutil.GatherAndCompare(reg, bytes.NewBufferString(`
        	            	# HELP cortex_grpc_connected_clients Number of clients connected to gRPC server
        	            	# TYPE cortex_grpc_connected_clients gauge
        	            	cortex_grpc_connected_clients 1

        	            	# HELP cortex_grpc_method_errors_total Number of clients connected to gRPC server
        	            	# TYPE cortex_grpc_method_errors_total counter
        	            	cortex_grpc_method_errors_total{method="/grpc.health.v1.Health/Check"} 1

        	            	# HELP cortex_grpc_request_size_bytes Size of gRPC requests.
        	            	# TYPE cortex_grpc_request_size_bytes histogram
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="1.048576e+06"} 1
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="2.62144e+06"} 1
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="5.24288e+06"} 1
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="1.048576e+07"} 2
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="2.62144e+07"} 2
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="5.24288e+07"} 2
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="1.048576e+08"} 2
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="2.62144e+08"} 2
        	            	cortex_grpc_request_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="+Inf"} 2
        	            	cortex_grpc_request_size_bytes_sum{method="/grpc.health.v1.Health/Check"} 8.388613e+06
        	            	cortex_grpc_request_size_bytes_count{method="/grpc.health.v1.Health/Check"} 2

        	            	# HELP cortex_grpc_response_size_bytes Size of gRPC responses.
        	            	# TYPE cortex_grpc_response_size_bytes histogram
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="1.048576e+06"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="2.62144e+06"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="5.24288e+06"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="1.048576e+07"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="2.62144e+07"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="5.24288e+07"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="1.048576e+08"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="2.62144e+08"} 1
        	            	cortex_grpc_response_size_bytes_bucket{method="/grpc.health.v1.Health/Check",le="+Inf"} 1
        	            	cortex_grpc_response_size_bytes_sum{method="/grpc.health.v1.Health/Check"} 7
        	            	cortex_grpc_response_size_bytes_count{method="/grpc.health.v1.Health/Check"} 1
	`), "cortex_grpc_connected_clients", "cortex_grpc_request_size_bytes", "cortex_grpc_response_size_bytes", "cortex_grpc_method_errors_total")
	require.NoError(t, err)

	closed = true
	require.NoError(t, conn.Close())
	err = testutil.GatherAndCompare(reg, bytes.NewBufferString(`
        	            	# HELP cortex_grpc_connected_clients Number of clients connected to gRPC server
        	            	# TYPE cortex_grpc_connected_clients gauge
        	            	cortex_grpc_connected_clients 0
	`), "cortex_grpc_connected_clients")
}

func generateString(t *testing.T, size int) string {
	// Use random bytes, to avoid compression.
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	for ix, b := range buf {
		if b < ' ' {
			b += ' '
		}
		b = b & 0x7f
		buf[ix] = b
	}
	return string(buf)
}
