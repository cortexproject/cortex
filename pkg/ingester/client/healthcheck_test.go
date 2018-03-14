package client

import (
	fmt "fmt"
	"testing"
	"time"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type mockIngester struct {
	IngesterClient
	happy  bool
	status grpc_health_v1.HealthCheckResponse_ServingStatus
}

func (i mockIngester) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	if !i.happy {
		return nil, fmt.Errorf("Fail")
	}
	return &grpc_health_v1.HealthCheckResponse{Status: i.status}, nil
}

func TestHealthCheck(t *testing.T) {
	tcs := []struct {
		ingester mockIngester
		hasError bool
	}{
		{mockIngester{happy: true, status: grpc_health_v1.HealthCheckResponse_UNKNOWN}, true},
		{mockIngester{happy: true, status: grpc_health_v1.HealthCheckResponse_SERVING}, false},
		{mockIngester{happy: true, status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, true},
		{mockIngester{happy: false, status: grpc_health_v1.HealthCheckResponse_UNKNOWN}, true},
		{mockIngester{happy: false, status: grpc_health_v1.HealthCheckResponse_SERVING}, true},
		{mockIngester{happy: false, status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, true},
	}
	for _, tc := range tcs {
		err := HealthCheck(tc.ingester, 50*time.Millisecond)
		hasError := err != nil
		if hasError != tc.hasError {
			t.Errorf("Expected error: %t, error: %v", tc.hasError, err)
		}
	}
}
