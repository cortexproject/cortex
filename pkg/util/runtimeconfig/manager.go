package runtimeconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// Loader loads the configuration from file.
type Loader func(r io.Reader) (interface{}, error)

// ManagerConfig holds the config for an Manager instance.
// It holds config related to loading per-tenant config.
type ManagerConfig struct {
	ReloadPeriod time.Duration `yaml:"period"`
	// LoadPath contains the path to the runtime config file, requires an
	// non-empty value
	LoadPath string `yaml:"file"`
	Loader   Loader `yaml:"-"`
}

type ExtensionConfig struct {
	Name     string
	LoadPath string
	Loader   Loader
}

// RegisterFlags registers flags.
func (mc *ManagerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&mc.LoadPath, "runtime-config.file", "", "File with the configuration that can be updated in runtime.")
	f.DurationVar(&mc.ReloadPeriod, "runtime-config.reload-period", 10*time.Second, "How often to check runtime config file.")
}

// Manager periodically reloads the configuration from a file, and keeps this
// configuration available for clients.
type Manager struct {
	services.Service

	cfg           ManagerConfig
	extensionCfgs map[string]ExtensionConfig `yaml:"-"`

	listenersMtx sync.Mutex
	// Keyed by config name.
	listeners map[string][]chan interface{}

	configMtx sync.RWMutex
	// Keyed by config name.
	config map[string]interface{}

	configLoadSuccess prometheus.Gauge
	configHash        *prometheus.GaugeVec
}

const DefaultConfigName = "__default__"

// NewRuntimeConfigManager creates an instance of Manager and starts reload config loop based on config
func NewRuntimeConfigManager(cfg ManagerConfig, registerer prometheus.Registerer) (*Manager, error) {
	if cfg.LoadPath == "" {
		return nil, errors.New("LoadPath is empty")
	}

	mgr := Manager{
		cfg:           cfg,
		extensionCfgs: make(map[string]ExtensionConfig),
		configLoadSuccess: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_runtime_config_last_reload_successful",
			Help: "Whether the last runtime-config reload attempt was successful.",
		}),
		configHash: promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_runtime_config_hash",
			Help: "Hash of the currently active runtime config file.",
		}, []string{"sha256"}),

		listeners: make(map[string][]chan interface{}, 1),
		config:    make(map[string]interface{}, 1),
	}

	mgr.Service = services.NewBasicService(mgr.start, mgr.loop, mgr.stop)
	return &mgr, nil
}

func (om *Manager) start(_ context.Context) error {
	if om.cfg.LoadPath != "" {
		if err := om.loadConfig(); err != nil {
			// Log but don't stop on error - we don't want to halt all ingesters because of a typo
			level.Error(util.Logger).Log("msg", "failed to load config", "err", err)
		}
	}
	return nil
}

// CreateListenerChannel creates new channel that can be used to receive new config values.
// If there is no receiver waiting for value when config manager tries to send the update,
// or channel buffer is full, update is discarded.
//
// When config manager is stopped, it closes all channels to notify receivers that they will
// not receive any more updates.
func (om *Manager) CreateListenerChannel(buffer int, cfgNames ...string) <-chan interface{} {
	ch := make(chan interface{}, buffer)

	if len(cfgNames) == 0 {
		cfgNames = []string{DefaultConfigName}
	}

	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for _, cfgName := range cfgNames {
		om.listeners[cfgName] = append(om.listeners[cfgName], ch)
	}

	return ch
}

// CloseListenerChannel removes given channel from list of channels to send notifications to and closes channel.
func (om *Manager) CloseListenerChannel(listener <-chan interface{}, cfgNames ...string) {
	if len(cfgNames) == 0 {
		cfgNames = []string{DefaultConfigName}
	}

	closed := false

	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

cfgs:
	for _, cfgName := range cfgNames {
		for ix, ch := range om.listeners[cfgName] {
			if ch == listener {
				om.listeners[cfgName] = append(om.listeners[cfgName][:ix], om.listeners[cfgName][ix+1:]...)
				if !closed {
					// Ensure that we only close the chan once.
					close(ch)
					closed = true
					continue cfgs
				}
			}
		}
	}
}

func (om *Manager) AddExtension(cfg ExtensionConfig) error {
	om.configMtx.Lock()
	defer om.configMtx.Unlock()

	if _, ok := om.extensionCfgs[cfg.Name]; ok {
		return errors.New("Extension is already registered")
	}

	om.extensionCfgs[cfg.Name] = cfg
	return nil
}

func (om *Manager) loop(ctx context.Context) error {
	if om.cfg.LoadPath == "" {
		level.Info(util.Logger).Log("msg", "runtime config disabled: file not specified")
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
				level.Error(util.Logger).Log("msg", "failed to load config", "err", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (om *Manager) loadConfig() error {
	cfg, hash, err := loadSingleConfig(om.cfg.LoadPath, om.cfg.Loader)
	if err != nil {
		om.configLoadSuccess.Set(0)
		return err
	}
	om.setConfig(cfg, DefaultConfigName)
	om.callListeners(cfg, DefaultConfigName)

	for _, extension := range om.extensionCfgs {
		cfg, hash, err = loadSingleConfig(extension.LoadPath, extension.Loader)
		if err != nil {
			om.configLoadSuccess.Set(0)
			return err
		}
		om.setConfig(cfg, extension.Name)
		om.callListeners(cfg, extension.Name)
	}

	om.configLoadSuccess.Set(1)

	// expose hash of runtime config
	om.configHash.Reset()
	om.configHash.WithLabelValues(fmt.Sprintf("%x", hash[:])).Set(1)

	return nil
}

// loadSingleConfig loads specified configuration file using the loader function.
// It returns the loaded file, a hash of the file, and potential errors.
func loadSingleConfig(path string, loader Loader) (interface{}, [32]byte, error) {
	var hash [32]byte
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, hash, err
	}
	hash = sha256.Sum256(buf)

	cfg, err := loader(bytes.NewReader(buf))
	if err != nil {
		return nil, hash, err
	}

	return cfg, hash, nil
}

func (om *Manager) setConfig(config interface{}, cfgName string) {
	om.configMtx.Lock()
	defer om.configMtx.Unlock()
	om.config[cfgName] = config
}

func (om *Manager) callListeners(newValue interface{}, cfgName string) {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for _, ch := range om.listeners[cfgName] {
		select {
		case ch <- newValue:
			// ok
		default:
			// nobody is listening or buffer full.
		}
	}
}

// Stop stops the Manager
func (om *Manager) stop(_ error) error {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for configName := range om.listeners {
		for _, ch := range om.listeners[configName] {
			close(ch)
		}
	}

	om.listeners = nil
	return nil
}

// GetConfig returns last loaded config value of the default config, possibly nil.
func (om *Manager) GetConfig() interface{} {
	om.configMtx.RLock()
	defer om.configMtx.RUnlock()

	return om.config[DefaultConfigName]
}
