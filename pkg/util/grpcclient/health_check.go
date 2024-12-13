package grpcclient

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/util/services"
)

var (
	unhealthyErr = status.Error(codes.Unavailable, "instance marked as unhealthy")
)

type HealthCheckConfig struct {
	*HealthCheckInterceptors `yaml:"-"`

	UnhealthyThreshold int64         `yaml:"unhealthy_threshold"`
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
}

// RegisterFlagsWithPrefix for Config.
func (cfg *HealthCheckConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Int64Var(&cfg.UnhealthyThreshold, prefix+".healthcheck.unhealthy-threshold", 0, "The number of consecutive failed health checks required before considering a target unhealthy. 0 means disabled.")
	f.DurationVar(&cfg.Timeout, prefix+".healthcheck.timeout", 1*time.Second, "The amount of time during which no response from a target means a failed health check.")
	f.DurationVar(&cfg.Interval, prefix+".healthcheck.interval", 5*time.Second, "The approximate amount of time between health checks of an individual target.")
}

type healthCheckClient struct {
	grpc_health_v1.HealthClient
	io.Closer
}

type healthCheckEntry struct {
	address        string
	clientConfig   *ConfigWithHealthCheck
	lastCheckTime  atomic.Time
	lastTickTime   atomic.Time
	unhealthyCount atomic.Int64

	healthCheckClientMutex sync.RWMutex
	healthCheckClient      *healthCheckClient
}

type HealthCheckInterceptors struct {
	services.Service
	logger log.Logger

	sync.RWMutex
	activeInstances map[string]*healthCheckEntry

	instanceGcTimeout   time.Duration
	healthClientFactory func(cc *grpc.ClientConn) (grpc_health_v1.HealthClient, io.Closer)
}

func NewHealthCheckInterceptors(logger log.Logger) *HealthCheckInterceptors {
	h := &HealthCheckInterceptors{
		logger:            logger,
		instanceGcTimeout: 2 * time.Minute,
		healthClientFactory: func(cc *grpc.ClientConn) (grpc_health_v1.HealthClient, io.Closer) {
			return grpc_health_v1.NewHealthClient(cc), cc
		},
		activeInstances: make(map[string]*healthCheckEntry),
	}

	h.Service = services.
		NewTimerService(time.Second, nil, h.iteration, nil).WithName("Grp Client HealthCheck Interceptors")
	return h
}

func (e *healthCheckEntry) isHealthy() bool {
	return e.unhealthyCount.Load() < e.clientConfig.HealthCheckConfig.UnhealthyThreshold
}

func (e *healthCheckEntry) recordHealth(err error) error {
	if err != nil {
		e.unhealthyCount.Inc()
	} else {
		e.unhealthyCount.Store(0)
	}

	return err
}

func (e *healthCheckEntry) tick() {
	e.lastTickTime.Store(time.Now())
}

func (e *healthCheckEntry) close() error {
	e.healthCheckClientMutex.Lock()
	defer e.healthCheckClientMutex.Unlock()

	if e.healthCheckClient != nil {
		err := e.healthCheckClient.Close()
		e.healthCheckClient = nil
		return err
	}

	return nil
}

func (e *healthCheckEntry) getClient(factory func(cc *grpc.ClientConn) (grpc_health_v1.HealthClient, io.Closer)) (*healthCheckClient, error) {
	e.healthCheckClientMutex.RLock()
	c := e.healthCheckClient
	e.healthCheckClientMutex.RUnlock()

	if c != nil {
		return c, nil
	}

	e.healthCheckClientMutex.Lock()
	defer e.healthCheckClientMutex.Unlock()

	if e.healthCheckClient == nil {
		dialOpts, err := e.clientConfig.Config.DialOption(nil, nil)
		if err != nil {
			return nil, err
		}
		conn, err := grpc.NewClient(e.address, dialOpts...)
		if err != nil {
			return nil, err
		}

		client, closer := factory(conn)
		e.healthCheckClient = &healthCheckClient{
			HealthClient: client,
			Closer:       closer,
		}
	}

	return e.healthCheckClient, nil
}

func (h *HealthCheckInterceptors) registeredInstances() []*healthCheckEntry {
	h.RLock()
	defer h.RUnlock()
	r := make([]*healthCheckEntry, 0, len(h.activeInstances))
	for _, i := range h.activeInstances {
		r = append(r, i)
	}

	return r
}

func (h *HealthCheckInterceptors) iteration(ctx context.Context) error {
	level.Debug(h.logger).Log("msg", "Performing health check", "registeredInstances", len(h.registeredInstances()))
	for _, instance := range h.registeredInstances() {
		if time.Since(instance.lastTickTime.Load()) >= h.instanceGcTimeout {
			h.Lock()
			if err := instance.close(); err != nil {
				level.Warn(h.logger).Log("msg", "Error closing health check", "err", err)
			}
			delete(h.activeInstances, instance.address)
			h.Unlock()
			continue
		}

		if time.Since(instance.lastCheckTime.Load()) < instance.clientConfig.HealthCheckConfig.Interval {
			continue
		}

		instance.lastCheckTime.Store(time.Now())

		go func(i *healthCheckEntry) {
			client, err := i.getClient(h.healthClientFactory)

			if err != nil {
				level.Error(h.logger).Log("msg", "error creating healthcheck client to perform healthcheck", "address", i.address, "err", err)
				return
			}

			if err := i.recordHealth(healthCheck(client, i.clientConfig.HealthCheckConfig.Timeout)); !i.isHealthy() {
				level.Warn(h.logger).Log("msg", "instance marked as unhealthy", "address", i.address, "err", err)
			}
		}(instance)
	}
	return nil
}

func (h *HealthCheckInterceptors) getOrAddHealthCheckEntry(address string, clientConfig *ConfigWithHealthCheck) *healthCheckEntry {
	h.RLock()
	e := h.activeInstances[address]
	h.RUnlock()

	if e != nil {
		return e
	}

	h.Lock()
	defer h.Unlock()

	if _, ok := h.activeInstances[address]; !ok {
		h.activeInstances[address] = &healthCheckEntry{
			address:      address,
			clientConfig: clientConfig,
		}
	}

	return h.activeInstances[address]
}

func (h *HealthCheckInterceptors) StreamClientInterceptor(clientConfig *ConfigWithHealthCheck) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		e := h.getOrAddHealthCheckEntry(cc.Target(), clientConfig)
		e.tick()
		if !e.isHealthy() {
			return nil, unhealthyErr
		}

		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (h *HealthCheckInterceptors) UnaryHealthCheckInterceptor(clientConfig *ConfigWithHealthCheck) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		e := h.getOrAddHealthCheckEntry(cc.Target(), clientConfig)
		e.tick()
		if !e.isHealthy() {
			return unhealthyErr
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func healthCheck(client grpc_health_v1.HealthClient, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ctx = user.InjectOrgID(ctx, "0")

	resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("failing healthcheck status: %s", resp.Status)
	}
	return nil
}
