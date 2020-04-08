package querier

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

func NewStoreGatewayClientFactory(cfg grpcclient.Config, reg prometheus.Registerer) client.PoolFactory {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "querier_storegateway_client_request_duration_seconds",
		Help:      "Time spent executing requests on store-gateway.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 4, 6),
	}, []string{"operation", "status_code"})

	return func(addr string) (client.PoolClient, error) {
		return DialStoreGatewayClient(cfg, addr, requestDuration)
	}
}

func DialStoreGatewayClient(cfg grpcclient.Config, addr string, requestDuration *prometheus.HistogramVec) (*storeGatewayClient, error) {
	opts := []grpc.DialOption{grpc.WithInsecure()}
	opts = append(opts, cfg.DialOption(grpcclient.Instrument(requestDuration))...)
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial store-gateway %s", addr)
	}

	return &storeGatewayClient{
		StoreGatewayClient: storegatewaypb.NewStoreGatewayClient(conn),
		HealthClient:       grpc_health_v1.NewHealthClient(conn),
		conn:               conn,
	}, nil
}

type storeGatewayClient struct {
	storegatewaypb.StoreGatewayClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *storeGatewayClient) Close() error {
	return c.conn.Close()
}
