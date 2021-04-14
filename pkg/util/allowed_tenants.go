package util

type AllowedTenants struct {
	// If empty, all tenants are enabled. If not empty, only tenants in the map are enabled.
	enabled map[string]struct{}

	// If empty, no tenants are disabled. If not empty, tenants in the map are disabled.
	disabled map[string]struct{}
}

func NewAllowedTenants(enabled []string, disabled []string) *AllowedTenants {
	a := &AllowedTenants{}

	if len(enabled) > 0 {
		a.enabled = make(map[string]struct{}, len(enabled))
		for _, u := range enabled {
			a.enabled[u] = struct{}{}
		}
	}

	if len(disabled) > 0 {
		a.disabled = make(map[string]struct{}, len(disabled))
		for _, u := range disabled {
			a.disabled[u] = struct{}{}
		}
	}

	return a
}

func (a *AllowedTenants) IsAllowed(tenantID string) bool {
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
