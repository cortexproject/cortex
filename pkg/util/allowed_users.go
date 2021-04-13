package util

type AllowedUsers struct {
	// If empty, all users are enabled. If not empty, only users in the map are enabled.
	enabled map[string]struct{}

	// If empty, no users are disabled. If not empty, users in the map are disabled.
	disabled map[string]struct{}
}

func NewAllowedUsers(enabled []string, disabled []string) *AllowedUsers {
	a := &AllowedUsers{}

	if len(enabled) > 0 {
		a.enabled = map[string]struct{}{}
		for _, u := range enabled {
			a.enabled[u] = struct{}{}
		}
	}

	if len(disabled) > 0 {
		a.disabled = map[string]struct{}{}
		for _, u := range disabled {
			a.disabled[u] = struct{}{}
		}
	}

	return a
}

func (a *AllowedUsers) IsAllowed(userID string) bool {
	if len(a.enabled) > 0 {
		if _, ok := a.enabled[userID]; !ok {
			return false
		}
	}

	if len(a.disabled) > 0 {
		if _, ok := a.disabled[userID]; ok {
			return false
		}
	}

	return true
}
