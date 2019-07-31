package testutils

import (
	"github.com/cortexproject/cortex/pkg/configs/storage/alerts"
	"github.com/cortexproject/cortex/pkg/configs/storage/rules"
)

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (alerts.AlertStore, rules.RuleStore, error)
	Teardown() error
}
