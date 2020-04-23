package querier

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

func newStoreGatewayClientFactory(cfg grpcclient.Config, reg prometheus.Registerer) client.PoolFactory {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "cortex",
		Name:        "storegateway_client_request_duration_seconds",
		Help:        "Time spent executing requests to the store-gateway.",
		Buckets:     prometheus.ExponentialBuckets(0.008, 4, 7),
		ConstLabels: prometheus.Labels{"client": "querier"},
	}, []string{"operation", "status_code"})

	return func(addr string) (client.PoolClient, error) {
		return dialStoreGatewayClient(cfg, addr, requestDuration)
	}
}

func dialStoreGatewayClient(cfg grpcclient.Config, addr string, requestDuration *prometheus.HistogramVec) (*storeGatewayClient, error) {
	opts := cfg.DialOption(grpcclient.Instrument(requestDuration))
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

func (c *storeGatewayClient) String() string {
	return c.conn.Target()
}

func newStoreGatewayClientPool(discovery client.PoolServiceDiscovery, clientCfg grpcclient.Config, logger log.Logger, reg prometheus.Registerer) *client.Pool {
	poolCfg := client.PoolConfig{
		CheckInterval:      time.Minute,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	clientsCount := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace:   "cortex",
		Name:        "storegateway_clients",
		Help:        "The current number of store-gateway clients in the pool.",
		ConstLabels: map[string]string{"client": "querier"},
	})

	return client.NewPool("store-gateway", poolCfg, discovery, newStoreGatewayClientFactory(clientCfg, reg), clientsCount, logger)
}
