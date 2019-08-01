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

func (lm *OverridesManager) loop() {
	ticker := time.NewTicker(lm.overridesReloadPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := lm.loadOverrides()
			if err != nil {
				level.Error(util.Logger).Log("msg", "failed to load limit overrides", "err", err)
			}
		case <-lm.quit:
			return
		}
	}
}

func (lm *OverridesManager) loadOverrides() error {
	overrides, err := lm.overridesLoader(lm.overridesReloadPath)
	if err != nil {
		overridesReloadSuccess.Set(0)
		return err
	}
	overridesReloadSuccess.Set(1)

	lm.overridesMtx.Lock()
	defer lm.overridesMtx.Unlock()
	lm.overrides = overrides
	return nil
}

// Stop stops the OverridesManager
func (lm *OverridesManager) Stop() {
	close(lm.quit)
}

// GetLimits returns Limits for a specific userID if its set otherwise the default Limits
func (lm *OverridesManager) GetLimits(userID string) interface{} {
	lm.overridesMtx.RLock()
	defer lm.overridesMtx.RUnlock()

	override, ok := lm.overrides[userID]
	if !ok {
		return lm.defaults
	}

	return override
}
