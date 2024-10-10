package grpcclient

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/util/services"
)

var (
	unhealthyErr = errors.New("instance marked as unhealthy")
)

type HealthCheckConfig struct {
	*HealthCheckInterceptors `yaml:"-"`

	UnhealthyThreshold int           `yaml:"unhealthy_threshold"`
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
}

// RegisterFlagsWithPrefix for Config.
func (cfg *HealthCheckConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.UnhealthyThreshold, prefix+".unhealthy-threshold", 0, "The number of consecutive failed health checks required before considering a target unhealthy. 0 means disabled.")
	f.DurationVar(&cfg.Timeout, prefix+".timeout", 1*time.Second, "The amount of time during which no response from a target means a failed health check.")
	f.DurationVar(&cfg.Interval, prefix+".interval", 5*time.Second, "The approximate amount of time between health checks of an individual target.")
}

type healthCheckEntry struct {
	address      string
	clientConfig *ConfigWithHealthCheck

	sync.RWMutex
	unhealthyCount int
	lastCheckTime  atomic.Time
	lastTickTime   atomic.Time
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
	e.RLock()
	defer e.RUnlock()
	return e.unhealthyCount < e.clientConfig.HealthCheckConfig.UnhealthyThreshold
}

func (e *healthCheckEntry) recordHealth(err error) error {
	e.Lock()
	defer e.Unlock()
	if err != nil {
		e.unhealthyCount++
	} else {
		e.unhealthyCount = 0
	}

	return err
}

func (e *healthCheckEntry) tick() {
	e.lastTickTime.Store(time.Now())
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
			delete(h.activeInstances, instance.address)
			h.Unlock()
			continue
		}

		if time.Since(instance.lastCheckTime.Load()) < instance.clientConfig.HealthCheckConfig.Interval {
			continue
		}

		instance.lastCheckTime.Store(time.Now())

		go func(i *healthCheckEntry) {
			dialOpts, err := i.clientConfig.Config.DialOption(nil, nil)
			if err != nil {
				level.Error(h.logger).Log("msg", "error creating dialOpts to perform healthcheck", "address", i.address, "err", err)
				return
			}
			conn, err := grpc.NewClient(i.address, dialOpts...)
			if err != nil {
				level.Error(h.logger).Log("msg", "error creating client to perform healthcheck", "address", i.address, "err", err)
				return
			}

			client, closer := h.healthClientFactory(conn)

			defer func() {
				if err := closer.Close(); err != nil {
					level.Warn(h.logger).Log("msg", "error closing connection", "address", i.address, "err", err)
				}
			}()

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
