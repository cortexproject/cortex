package distributor

import (
	"flag"
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

// AlertmanagerClientsPool is the interface used to get the client from the pool for a specified address.
type AlertmanagerClientsPool interface {
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

// AlertmanagerClientConfig is the configuration struct for the alertmanager client.
type AlertmanagerClientConfig struct {
	TLSEnabled bool             `yaml:"tls_enabled"`
	TLS        tls.ClientConfig `yaml:",inline"`
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *AlertmanagerClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.TLSEnabled, prefix+".tls-enabled", cfg.TLSEnabled, "Enable TLS in the GRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix, f)
}

type alertmanagerClientsPool struct {
	pool *client.Pool
}

func newAlertmanagerClientsPool(discovery client.PoolServiceDiscovery, amClientCfg AlertmanagerClientConfig, logger log.Logger, reg prometheus.Registerer) AlertmanagerClientsPool {
	// We prefer sane defaults instead of exposing further config options.
	// TODO: Figure out if these defaults make sense.
	grpcCfg := grpcclient.Config{
		MaxRecvMsgSize:      100 << 20,
		MaxSendMsgSize:      16 << 20,
		GRPCCompression:     "",
		RateLimit:           0,
		RateLimitBurst:      0,
		BackoffOnRatelimits: false,
		TLSEnabled:          amClientCfg.TLSEnabled,
		TLS:                 amClientCfg.TLS,
	}

	rd := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "cortex",
		Name:        "alertmanager_client_request_duration_seconds",
		Help:        "Time spent executing requests to the alertmanager.",
		Buckets:     prometheus.ExponentialBuckets(0.008, 4, 7),
		ConstLabels: prometheus.Labels{"client": "alertmanager-distributor"},
	}, []string{"operation", "status_code"})

	factory := func(addr string) (client.PoolClient, error) {
		return dialAlertmanagerClient(grpcCfg, addr, rd)
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

	return &alertmanagerClientsPool{pool: client.NewPool("alertmanager", poolCfg, discovery, factory, clientsCount, logger)}
}

func (f *alertmanagerClientsPool) GetClientFor(addr string) (AlertmanagerClient, error) {
	c, err := f.pool.GetClientFor(addr)
	if err != nil {
		return nil, err
	}
	return c.(AlertmanagerClient), nil
}

func dialAlertmanagerClient(cfg grpcclient.Config, addr string, rd *prometheus.HistogramVec) (*alertmanagerClient, error) {
	opts, err := cfg.DialOption(grpcclient.Instrument(rd))
	if err != nil {
		return nil, err
	}
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
