package util

import (
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var overridesReloadSuccess = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "cortex_overrides_last_reload_successful",
	Help: "Whether the last overrides reload attempt was successful.",
})

// OverridesLoader loads the configuration from file.
type OverridesLoader func(filename string) (interface{}, error)

// OverridesListener gets notified when new overrides is loaded
type OverridesListener func(newConfig interface{})

// OverridesManagerConfig holds the config for an OverridesManager instance.
// It holds config related to loading per-tenant overrides.
type OverridesManagerConfig struct {
	OverridesReloadPeriod time.Duration
	OverridesLoadPath     string
	OverridesLoader       OverridesLoader
}

// OverridesManager periodically reloads configuration from a file, and keeps this
// configuration available for clients.
type OverridesManager struct {
	cfg  OverridesManagerConfig
	quit chan struct{}

	listenersMtx sync.Mutex
	listeners    []OverridesListener

	overridesMtx sync.RWMutex
	overrides    interface{}
}

// NewOverridesManager creates an instance of OverridesManager and starts reload overrides loop based on config
func NewOverridesManager(cfg OverridesManagerConfig) (*OverridesManager, error) {
	overridesManager := OverridesManager{
		cfg:  cfg,
		quit: make(chan struct{}),
	}

	if cfg.OverridesLoadPath != "" {
		if err := overridesManager.loadOverrides(); err != nil {
			// Log but don't stop on error - we don't want to halt all ingesters because of a typo
			level.Error(Logger).Log("msg", "failed to load overrides", "err", err)
		}
		go overridesManager.loop()
	} else {
		level.Info(Logger).Log("msg", "overrides disabled")
	}

	return &overridesManager, nil
}

// AddListener registers new listener function, that will receive updates configuration.
// Listener is called asynchronously to avoid blocking main reloading loop.
func (om *OverridesManager) AddListener(l OverridesListener) {
	if l == nil {
		panic("nil listener")
	}

	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	om.listeners = append(om.listeners, l)
}

func (om *OverridesManager) loop() {
	ticker := time.NewTicker(om.cfg.OverridesReloadPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := om.loadOverrides()
			if err != nil {
				// Log but don't stop on error - we don't want to halt all ingesters because of a typo
				level.Error(Logger).Log("msg", "failed to load overrides", "err", err)
			}
		case <-om.quit:
			return
		}
	}
}

func (om *OverridesManager) loadOverrides() error {
	overrides, err := om.cfg.OverridesLoader(om.cfg.OverridesLoadPath)
	if err != nil {
		overridesReloadSuccess.Set(0)
		return err
	}
	overridesReloadSuccess.Set(1)

	om.setOverrides(overrides)
	om.callListeners(overrides)

	return nil
}

func (om *OverridesManager) setOverrides(overrides interface{}) {
	om.overridesMtx.Lock()
	defer om.overridesMtx.Unlock()
	om.overrides = overrides
}

func (om *OverridesManager) callListeners(newValue interface{}) {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()
	for _, l := range om.listeners {
		go l(newValue)
	}
}

// Stop stops the OverridesManager
func (om *OverridesManager) Stop() {
	close(om.quit)
}

// GetOverrides returns last loaded overrides value, possibly nil.
func (om *OverridesManager) GetOverrides() interface{} {
	om.overridesMtx.RLock()
	defer om.overridesMtx.RUnlock()

	return om.overrides
}
