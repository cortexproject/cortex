package runtimeconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Loader loads the configuration from file.
type Loader func(r io.Reader) (interface{}, error)

// Config holds the config for an Manager instance.
// It holds config related to loading per-tenant config.
type Config struct {
	ReloadPeriod time.Duration `yaml:"period"`
	// LoadPath contains the path to the runtime config file, requires an
	// non-empty value
	LoadPath               string                  `yaml:"file"`
	TenantLimits           tenantLimitsConfig      `yaml:"overrides" doc:"description=Overrides default global limits (defined in limits_config) on a per-tenant basis. Specify tenant-specific limits using the same fields available in limits_config. Each tenant is defined as a key-value pair, where the key is the tenant ID and the value is an object with tenant-specific limits. Refer to the https://cortexmetrics.io/docs/configuration/configuration-file/#limits_config documentation for a description of available fields and to https://cortexmetrics.io/docs/configuration/arguments/#runtime-configuration-file for the example usage."`
	Multi                  kv.MultiRuntimeConfig   `yaml:"multi_kv_config"`
	IngesterChunkStreaming bool                    `yaml:"ingester_stream_chunks_when_using_blocks"`
	IngesterLimits         ingester.InstanceLimits `yaml:"ingester_limits"`
	Loader                 Loader                  `yaml:"-"`
}

// RegisterFlags registers flags.
func (mc *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&mc.LoadPath, "runtime-config.file", "", "File with the configuration that can be updated in runtime.")
	f.DurationVar(&mc.ReloadPeriod, "runtime-config.reload-period", 10*time.Second, "How often to check runtime config file.")
	mc.TenantLimits.registerFlags(f)
	mc.IngesterLimits.RegisterFlagsWithPrefix("runtime-config.ingester-limits", f)
	mc.Multi.RegisterFlagsWithPrefix("runtime-config.multi-kv-config", f)
	f.BoolVar(&mc.IngesterChunkStreaming, "runtime-config.ingester-stream-chunks-when-using-blocks", false, "Enable streaming entire chunks instead of individual samples to the querier")
}

type tenantLimitsConfig struct {
	perTenantLimits map[string]validation.Limits
}

func (cfg *tenantLimitsConfig) registerFlags(f *flag.FlagSet) {
	for key, value := range cfg.perTenantLimits {
		f.StringVar(&key, "runtime-config.override.tenant-id", "", "Specifices the tenant id")
		value.RegisterFlags(f)
	}
}

// Manager periodically reloads the configuration from a file, and keeps this
// configuration available for clients.
type Manager struct {
	services.Service

	cfg    Config
	logger log.Logger

	listenersMtx sync.Mutex
	listeners    []chan interface{}

	configMtx sync.RWMutex
	config    interface{}

	configLoadSuccess prometheus.Gauge
	configHash        *prometheus.GaugeVec
}

// New creates an instance of Manager and starts reload config loop based on config
func New(cfg Config, registerer prometheus.Registerer, logger log.Logger) (*Manager, error) {
	if cfg.LoadPath == "" {
		return nil, errors.New("LoadPath is empty")
	}

	mgr := Manager{
		cfg: cfg,
		configLoadSuccess: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "runtime_config_last_reload_successful",
			Help: "Whether the last runtime-config reload attempt was successful.",
		}),
		configHash: promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "runtime_config_hash",
			Help: "Hash of the currently active runtime config file.",
		}, []string{"sha256"}),
		logger: logger,
	}

	mgr.Service = services.NewBasicService(mgr.starting, mgr.loop, mgr.stopping)
	return &mgr, nil
}

func (om *Manager) starting(_ context.Context) error {
	if om.cfg.LoadPath == "" {
		return nil
	}

	return errors.Wrap(om.loadConfig(), "failed to load runtime config")
}

// CreateListenerChannel creates new channel that can be used to receive new config values.
// If there is no receiver waiting for value when config manager tries to send the update,
// or channel buffer is full, update is discarded.
//
// When config manager is stopped, it closes all channels to notify receivers that they will
// not receive any more updates.
func (om *Manager) CreateListenerChannel(buffer int) <-chan interface{} {
	ch := make(chan interface{}, buffer)

	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	om.listeners = append(om.listeners, ch)
	return ch
}

// CloseListenerChannel removes given channel from list of channels to send notifications to and closes channel.
func (om *Manager) CloseListenerChannel(listener <-chan interface{}) {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for ix, ch := range om.listeners {
		if ch == listener {
			om.listeners = append(om.listeners[:ix], om.listeners[ix+1:]...)
			close(ch)
			break
		}
	}
}

func (om *Manager) loop(ctx context.Context) error {
	if om.cfg.LoadPath == "" {
		level.Info(om.logger).Log("msg", "runtime config disabled: file not specified")
		<-ctx.Done()
		return nil
	}

	ticker := time.NewTicker(om.cfg.ReloadPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := om.loadConfig()
			if err != nil {
				// Log but don't stop on error - we don't want to halt all ingesters because of a typo
				level.Error(om.logger).Log("msg", "failed to load config", "err", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// loadConfig loads configuration using the loader function, and if successful,
// stores it as current configuration and notifies listeners.
func (om *Manager) loadConfig() error {
	buf, err := os.ReadFile(om.cfg.LoadPath)
	if err != nil {
		om.configLoadSuccess.Set(0)
		return errors.Wrap(err, "read file")
	}
	hash := sha256.Sum256(buf)

	cfg, err := om.cfg.Loader(bytes.NewReader(buf))
	if err != nil {
		om.configLoadSuccess.Set(0)
		return errors.Wrap(err, "load file")
	}
	om.configLoadSuccess.Set(1)

	om.setConfig(cfg)
	om.callListeners(cfg)

	// expose hash of runtime config
	om.configHash.Reset()
	om.configHash.WithLabelValues(fmt.Sprintf("%x", hash[:])).Set(1)

	return nil
}

func (om *Manager) setConfig(config interface{}) {
	om.configMtx.Lock()
	defer om.configMtx.Unlock()
	om.config = config
}

func (om *Manager) callListeners(newValue interface{}) {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for _, ch := range om.listeners {
		select {
		case ch <- newValue:
			// ok
		default:
			// nobody is listening or buffer full.
		}
	}
}

// Stop stops the Manager
func (om *Manager) stopping(_ error) error {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for _, ch := range om.listeners {
		close(ch)
	}
	om.listeners = nil
	return nil
}

// GetConfig returns last loaded config value, possibly nil.
func (om *Manager) GetConfig() interface{} {
	om.configMtx.RLock()
	defer om.configMtx.RUnlock()

	return om.config
}
