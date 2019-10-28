package servicediscovery

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// RegistryLifecycler registers a service instance to a centralized registry
// stored in the KVStore and keeps the instance heartbeat update.
//
// The lifecycler has been implemented to reduce the operations on the KVStore,
// thus it doesn't watch for updates on the registry, but updates are picked up
// only at the next heartbeat period. The maximum propagation time of a change
// in the registry should be expected to be not greater than the heartbeat period,
// assuming no issues with the KVStore.
type RegistryLifecycler struct {
	serviceName string
	kvStore     kv.Client
	cfg         RegistryLifecyclerConfig
	logger      log.Logger

	// Stats (updated every heartbeat period)
	statsLock    sync.Mutex
	healthyCount int

	// Used to signal the loop to quit
	cancel context.CancelFunc
	done   sync.WaitGroup
}

// RegistryLifecyclerConfig holds the service registry lifecycler config
type RegistryLifecyclerConfig struct {
	InstanceID       string        `yaml:"instance_id"`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`
	KVStore          kv.Config     `yaml:"kvstore,omitempty"`
}

// RegisterFlags adds the flags with empty prefix (used in tests)
func (cfg *RegistryLifecyclerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RegistryLifecyclerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("failed to get hostname: %v", err))
	}

	f.StringVar(&cfg.InstanceID, prefix+"instance-id", hostname, "Unique service instance ID across the cluster.")
	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 10*time.Second, "Registry update frequency.")
	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"heartbeat-timeout", 60*time.Second, "Timeout after which an instance is considered not alive anymore.")

	cfg.KVStore.RegisterFlagsWithPrefix(prefix, f)
}

// NewRegistryLifecycler makes a new service registry lifecycler,
// which keeps a service instance updated within the registry
func NewRegistryLifecycler(serviceName string, cfg RegistryLifecyclerConfig, logger log.Logger) (*RegistryLifecycler, error) {
	kvStore, err := kv.NewClient(cfg.KVStore, GetServiceRegistryCodec())
	if err != nil {
		return nil, err
	}

	return NewRegistryLifecyclerWithKVStore(serviceName, cfg, kvStore, logger)
}

// NewRegistryLifecyclerWithKVStore makes a new service registry lifecycler,
// allowing to specify the KVStore in input
func NewRegistryLifecyclerWithKVStore(serviceName string, cfg RegistryLifecyclerConfig, kvStore kv.Client, logger log.Logger) (*RegistryLifecycler, error) {
	ctx, cancel := context.WithCancel(context.Background())

	r := &RegistryLifecycler{
		serviceName: serviceName,
		kvStore:     kvStore,
		cfg:         cfg,
		logger:      logger,
		cancel:      cancel,
	}

	// Start the loop
	r.done.Add(1)
	go r.loop(ctx)

	return r, nil
}

// Stop updating the service in the registry and removes it
func (r *RegistryLifecycler) Stop() {
	// Quit the loop and wait until done
	r.cancel()
	r.done.Wait()

	// Remove the instance from the registry
	if err := r.removeInstance(context.Background()); err != nil {
		level.Warn(r.logger).Log("msg", fmt.Sprintf("failed to remove service instance %s from the %s registry", r.cfg.InstanceID, r.serviceName), "err", err)
	} else {
		level.Info(r.logger).Log("msg", fmt.Sprintf("removed service instance %s form the %s registry", r.cfg.InstanceID, r.serviceName))
	}
}

// HealthyCount returns the number of healthy service instances in the registry,
// updated every heartbeat period
func (r *RegistryLifecycler) HealthyCount() int {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()

	return r.healthyCount
}

func (r *RegistryLifecycler) loop(ctx context.Context) {
	defer r.done.Done()

	// Initial sync to register the service
	if err := r.updateInstance(ctx); err != nil {
		level.Warn(r.logger).Log("msg", fmt.Sprintf("failed to register service instance %s to the %s registry", r.cfg.InstanceID, r.serviceName), "err", err)
	}

	// Introduce a random delay to avoid all service instances update the registry
	// at the same time if they're started all together
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Duration(rand.Int63n(int64(r.cfg.HeartbeatPeriod)))):
		break
	}

	ticker := time.NewTicker(r.cfg.HeartbeatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.updateInstance(ctx); err != nil {
				level.Warn(r.logger).Log("msg", fmt.Sprintf("failed to update service instance %s in the %s registry", r.cfg.InstanceID, r.serviceName), "err", err)
			}
		}
	}
}

func (r *RegistryLifecycler) updateInstance(ctx context.Context) error {
	var registryDesc *ServiceRegistryDesc

	err := r.kvStore.CAS(ctx, r.serviceName, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			registryDesc = NewServiceRegistryDesc()
			level.Info(r.logger).Log("msg", fmt.Sprintf("service registry for %s is empty, creating a new one", r.serviceName))
		} else {
			registryDesc = in.(*ServiceRegistryDesc)
		}

		// Ensure the service instance is registered and update its heartbeat
		created := registryDesc.ensureInstance(r.cfg.InstanceID, time.Now())
		if created {
			level.Info(r.logger).Log("msg", fmt.Sprintf("service registry for %s doesn't contain %s, registering it", r.serviceName, r.cfg.InstanceID))
		}

		// Remove expired service instances. Count the remaining one, which are the healthy
		registryDesc.removeExpired(r.cfg.HeartbeatTimeout, time.Now())

		return registryDesc, true, nil
	})

	// Update stats
	if err == nil {
		r.statsLock.Lock()
		r.healthyCount = registryDesc.count()
		r.statsLock.Unlock()
	}

	return err
}

func (r *RegistryLifecycler) removeInstance(ctx context.Context) error {
	return r.kvStore.CAS(ctx, r.serviceName, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			// Shouldn't happen, but we've nothing to do anyway
			return nil, false, fmt.Errorf("service registry for %s is empty, skipping removing %s instance from it", r.serviceName, r.cfg.InstanceID)
		}

		registryDesc := in.(*ServiceRegistryDesc)
		registryDesc.removeInstance(r.cfg.InstanceID)

		return registryDesc, true, nil
	})
}
