package util

import (
	"context"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type AllowedTenantConfig struct {
	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants"`
}

// AllowedTenants that can answer whether tenant is allowed or not based on configuration.
// Default value (nil) allows all tenants.
type AllowedTenants struct {
	services.Service

	// If empty, all tenants are enabled. If not empty, only tenants in the map are enabled.
	enabled map[string]struct{}

	// If empty, no tenants are disabled. If not empty, tenants in the map are disabled.
	disabled map[string]struct{}

	allowedTenantConfigFn func() *AllowedTenantConfig
	defaultCfg            *AllowedTenantConfig
	m                     sync.RWMutex
}

// NewAllowedTenants builds new allowed tenants based on enabled and disabled tenants.
// If there are any enabled tenants, then only those tenants are allowed.
// If there are any disabled tenants, then tenant from that list, that would normally be allowed, is disabled instead.
func NewAllowedTenants(cfg AllowedTenantConfig, allowedTenantConfigFn func() *AllowedTenantConfig) *AllowedTenants {
	if allowedTenantConfigFn == nil {
		allowedTenantConfigFn = func() *AllowedTenantConfig { return &cfg }
	}

	a := &AllowedTenants{
		allowedTenantConfigFn: allowedTenantConfigFn,
		defaultCfg:            &cfg,
	}

	a.setConfig(allowedTenantConfigFn())

	a.Service = services.NewBasicService(nil, a.running, nil)

	return a
}

func (a *AllowedTenants) running(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if a.allowedTenantConfigFn == nil {
				c := a.allowedTenantConfigFn()
				if c == nil {
					c = a.defaultCfg
				}
				a.setConfig(c)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (a *AllowedTenants) setConfig(cfg *AllowedTenantConfig) {
	if a == nil {
		return
	}

	a.m.Lock()
	defer a.m.Unlock()
	if len(cfg.EnabledTenants) > 0 {
		a.enabled = make(map[string]struct{}, len(cfg.EnabledTenants))
		for _, u := range cfg.EnabledTenants {
			a.enabled[u] = struct{}{}
		}
	} else {
		cfg.EnabledTenants = nil
	}

	if len(cfg.DisabledTenants) > 0 {
		a.disabled = make(map[string]struct{}, len(cfg.DisabledTenants))
		for _, u := range cfg.DisabledTenants {
			a.disabled[u] = struct{}{}
		}
	} else {
		a.disabled = nil
	}
}

func (a *AllowedTenants) IsAllowed(tenantID string) bool {
	if a == nil {
		return true
	}

	a.m.RLock()
	defer a.m.RUnlock()

	if len(a.enabled) > 0 {
		if _, ok := a.enabled[tenantID]; !ok {
			return false
		}
	}

	if len(a.disabled) > 0 {
		if _, ok := a.disabled[tenantID]; ok {
			return false
		}
	}

	return true
}
