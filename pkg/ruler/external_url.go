package ruler

import (
	"sync"
)

// userExternalURL tracks per-user resolved external URLs and detects changes.
type userExternalURL struct {
	global string
	limits RulesLimits

	mtx   sync.Mutex
	users map[string]string
}

func newUserExternalURL(global string, limits RulesLimits) *userExternalURL {
	return &userExternalURL{
		global: global,
		limits: limits,

		mtx:   sync.Mutex{},
		users: map[string]string{},
	}
}

func (e *userExternalURL) update(userID string) (string, bool) {
	tenantURL := e.limits.RulerExternalURL(userID)
	resolved := e.global
	if tenantURL != "" {
		resolved = tenantURL
	}

	e.mtx.Lock()
	defer e.mtx.Unlock()

	if prev, ok := e.users[userID]; ok && prev == resolved {
		return resolved, false
	}

	e.users[userID] = resolved
	return resolved, true
}

func (e *userExternalURL) remove(user string) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	delete(e.users, user)
}

func (e *userExternalURL) cleanup() {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	for user := range e.users {
		delete(e.users, user)
	}
}
