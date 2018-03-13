package client

import (
	fmt "fmt"
	"time"

	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// HealthCheck will check if the client is still healthy, returning an error if it is not
func HealthCheck(client IngesterClient, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ctx = user.InjectOrgID(ctx, "0")

	resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("Failing healthcheck status: %s", resp.Status)
	}
	return nil
}
