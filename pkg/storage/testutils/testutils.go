package testutils

import (
	alertStore "github.com/cortexproject/cortex/pkg/alertmanager/storage"
	"github.com/cortexproject/cortex/pkg/ruler/store"
)

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (alertStore.AlertStore, store.RuleStore, error)
	Teardown() error
}
