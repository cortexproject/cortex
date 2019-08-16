package validation

import (
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var overridesReloadSuccess = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "cortex_overrides_last_reload_successful",
	Help: "Whether the last overrides reload attempt was successful.",
})

func init() {
	overridesReloadSuccess.Set(1) // Default to 1
}

type overridesLoader func(string) (map[string]interface{}, error)

// LimitsUnmarshaler helps in unmarshaling limits from yaml
type LimitsUnmarshaler interface {
	UnmarshalYAML(unmarshal func(interface{}) error) error
}

// OverridesManager manages default and per user limits i.e overrides.
// It can periodically keep reloading overrides based on config.
type OverridesManager struct {
	overridesReloadPeriod time.Duration
	overridesReloadPath   string
	overridesLoader       overridesLoader

	defaults     interface{}
	overrides    map[string]interface{}
	overridesMtx sync.RWMutex
	quit         chan struct{}
}

// NewOverridesManager creates an instance of OverridesManager and starts reload overrides loop based on config
func NewOverridesManager(perTenantOverridePeriod time.Duration, overridesReloadPath string, overridesLoader overridesLoader,
	defaults interface{}) (*OverridesManager, error) {
	overridesManager := OverridesManager{
		overridesLoader:       overridesLoader,
		overridesReloadPeriod: perTenantOverridePeriod,
		overridesReloadPath:   overridesReloadPath,
		defaults:              defaults,
	}

	if overridesReloadPath != "" {
		overridesManager.loop()
	} else {
		level.Info(util.Logger).Log("msg", "per-tenant overrides disabled")
	}

	return &overridesManager, nil
}

func (om *OverridesManager) loop() {
	ticker := time.NewTicker(om.overridesReloadPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := om.loadOverrides()
			if err != nil {
				level.Error(util.Logger).Log("msg", "failed to load limit overrides", "err", err)
			}
		case <-om.quit:
			return
		}
	}
}

func (om *OverridesManager) loadOverrides() error {
	overrides, err := om.overridesLoader(om.overridesReloadPath)
	if err != nil {
		overridesReloadSuccess.Set(0)
		return err
	}
	overridesReloadSuccess.Set(1)

	om.overridesMtx.Lock()
	defer om.overridesMtx.Unlock()
	om.overrides = overrides
	return nil
}

// Stop stops the OverridesManager
func (om *OverridesManager) Stop() {
	close(om.quit)
}

// GetLimits returns Limits for a specific userID if its set otherwise the default Limits
func (om *OverridesManager) GetLimits(userID string) interface{} {
	om.overridesMtx.RLock()
	defer om.overridesMtx.RUnlock()

	override, ok := om.overrides[userID]
	if !ok {
		return om.defaults
	}

	return override
}
