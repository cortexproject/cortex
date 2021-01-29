package distributor

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertmanagerpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/tls"
)

// AlertmanagerClientFactory is the interface used to get the clients for the given addresses.
type AlertmanagerClientFactory interface {
	// GetClientFor returns the alertmanager client for the given address.
	GetClientFor(addr string) (AlertmanagerClient, error)
}

// AlertmanagerClient is the interface that should be implemented by any client used to read/write data to an alertmanager via GRPC.
type AlertmanagerClient interface {
	alertmanagerpb.AlertmanagerClient

	// RemoteAddress returns the address of the remote alertmanager and is used to uniquely
	// identify an alertmanager instance.
	RemoteAddress() string
}

type PooledAlertmanagerClientFactory struct {
	pool *client.Pool
}

func newPooledAlertmanagerClientFactory(discovery client.PoolServiceDiscovery, tlsCfg tls.ClientConfig, logger log.Logger, reg prometheus.Registerer) AlertmanagerClientFactory {
	// We prefer sane defaults instead of exposing further config options.
	// TODO: Figure out if these defaults make sense.
	grpcCfg := grpcclient.Config{
		MaxRecvMsgSize:      100 << 20,
		MaxSendMsgSize:      16 << 20,
		GRPCCompression:     "",
		RateLimit:           0,
		RateLimitBurst:      0,
		BackoffOnRatelimits: false,
	}

	rd := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "cortex",
		Name:        "alertmanager_client_request_duration_seconds",
		Help:        "Time spent executing requests to the alertmanager.",
		Buckets:     prometheus.ExponentialBuckets(0.008, 4, 7),
		ConstLabels: prometheus.Labels{"client": "alertmanager-distributor"},
	}, []string{"operation", "status_code"})

	factory := func(addr string) (client.PoolClient, error) {
		return dialAlertmanagerClient(grpcCfg, tlsCfg, addr, rd)
	}

	poolCfg := client.PoolConfig{
		CheckInterval:      time.Minute,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	clientsCount := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "alertmanager_distributor_alertmanager_clients",
		Help:      "The current number of alertmanager clients in the pool.",
	})

	return &PooledAlertmanagerClientFactory{pool: client.NewPool("alertmanager", poolCfg, discovery, factory, clientsCount, logger)}
}

func (f *PooledAlertmanagerClientFactory) GetClientFor(addr string) (AlertmanagerClient, error) {
	c, err := f.pool.GetClientFor(addr)
	if err != nil {
		return nil, err
	}
	return c.(AlertmanagerClient), nil
}

func dialAlertmanagerClient(cfg grpcclient.Config, tlsCfg tls.ClientConfig, addr string, rd *prometheus.HistogramVec) (*alertmanagerClient, error) {
	opts, err := tlsCfg.GetGRPCDialOptions()
	if err != nil {
		return nil, err
	}
	opts = append(opts, cfg.DialOption(grpcclient.Instrument(rd))...)
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial alertmanager %s", addr)
	}

	return &alertmanagerClient{
		AlertmanagerClient: alertmanagerpb.NewAlertmanagerClient(conn),
		HealthClient:       grpc_health_v1.NewHealthClient(conn),
		conn:               conn,
	}, nil
}

type alertmanagerClient struct {
	alertmanagerpb.AlertmanagerClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *alertmanagerClient) Close() error {
	return c.conn.Close()
}

func (c *alertmanagerClient) String() string {
	return c.RemoteAddress()
}

func (c *alertmanagerClient) RemoteAddress() string {
	return c.conn.Target()
}
