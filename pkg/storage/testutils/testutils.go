package testutils

import (
	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/ruler"
)

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (alertmanager.AlertStore, ruler.RuleStore, error)
	Teardown() error
}
