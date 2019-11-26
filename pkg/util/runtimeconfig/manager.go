package runtimeconfig

import (
	"flag"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var overridesReloadSuccess = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "cortex_overrides_last_reload_successful",
	Help: "Whether the last config reload attempt was successful.",
})

// Loader loads the configuration from file.
type Loader func(filename string) (interface{}, error)

// Listener gets notified when new config is loaded
type Listener func(newConfig interface{})

// ManagerConfig holds the config for an Manager instance.
// It holds config related to loading per-tenant config.
type ManagerConfig struct {
	ReloadPeriod time.Duration `yaml:"period"`
	LoadPath     string        `yaml:"file"`
	Loader       Loader
}

func (omc *ManagerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&omc.LoadPath, "runtime-config.file", "", "File with the configuration that can be updated in runtime.")
	f.DurationVar(&omc.ReloadPeriod, "runtime-config.reload-period", 10*time.Second, "How often to check runtime config file.")
}

// Manager periodically reloads the configuration from a file, and keeps this
// configuration available for clients.
type Manager struct {
	cfg  ManagerConfig
	quit chan struct{}

	listenersMtx sync.Mutex
	listeners    []Listener

	configMtx sync.RWMutex
	config    interface{}
}

// NewRuntimeConfigManager creates an instance of Manager and starts reload config loop based on config
func NewRuntimeConfigManager(cfg ManagerConfig) (*Manager, error) {
	mgr := Manager{
		cfg:  cfg,
		quit: make(chan struct{}),
	}

	if cfg.LoadPath != "" {
		if err := mgr.loadConfig(); err != nil {
			// Log but don't stop on error - we don't want to halt all ingesters because of a typo
			level.Error(util.Logger).Log("msg", "failed to load config", "err", err)
		}
		go mgr.loop()
	} else {
		level.Info(util.Logger).Log("msg", "config disabled")
	}

	return &mgr, nil
}

// AddListener registers new listener function, that will receive updates configuration.
// Listener is called asynchronously to avoid blocking main reloading loop.
func (om *Manager) AddListener(l Listener) {
	if l == nil {
		panic("nil listener")
	}

	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	om.listeners = append(om.listeners, l)
}

func (om *Manager) loop() {
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
		case <-om.quit:
			return
		}
	}
}

func (om *Manager) loadConfig() error {
	cfg, err := om.cfg.Loader(om.cfg.LoadPath)
	if err != nil {
		overridesReloadSuccess.Set(0)
		return err
	}
	overridesReloadSuccess.Set(1)

	om.setConfig(cfg)
	om.callListeners(cfg)

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
	for _, l := range om.listeners {
		go l(newValue)
	}
}

// Stop stops the Manager
func (om *Manager) Stop() {
	close(om.quit)
}

// GetConfig returns last loaded config value, possibly nil.
func (om *Manager) GetConfig() interface{} {
	om.configMtx.RLock()
	defer om.configMtx.RUnlock()

	return om.config
}
