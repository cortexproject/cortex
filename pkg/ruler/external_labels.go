package ruler

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
)

// userExternalLabels checks and merges per-user external labels with global external labels.
type userExternalLabels struct {
	global  labels.Labels
	limits  RulesLimits
	builder *labels.Builder

	mtx   sync.Mutex
	users map[string]labels.Labels
}

func newUserExternalLabels(global labels.Labels, limits RulesLimits) *userExternalLabels {
	return &userExternalLabels{
		global:  global,
		limits:  limits,
		builder: labels.NewBuilder(nil),

		mtx:   sync.Mutex{},
		users: map[string]labels.Labels{},
	}
}

func (e *userExternalLabels) get(userID string) (labels.Labels, bool) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	lset, ok := e.users[userID]
	return lset, ok
}

func (e *userExternalLabels) update(userID string) (labels.Labels, bool) {
	lset := e.limits.RulerExternalLabels(userID)

	e.mtx.Lock()
	defer e.mtx.Unlock()

	e.builder.Reset(e.global)
	for _, l := range lset {
		e.builder.Set(l.Name, l.Value)
	}
	lset = e.builder.Labels()

	if !labels.Equal(e.users[userID], lset) {
		e.users[userID] = lset
		return lset, true
	}
	return lset, false
}

func (e *userExternalLabels) remove(user string) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	delete(e.users, user)
}

func (e *userExternalLabels) cleanup() {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	for user := range e.users {
		delete(e.users, user)
	}
}
