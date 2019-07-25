package testutils

import (
	"github.com/cortexproject/cortex/pkg/storage/rules"
)

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (rules.RuleStore, error)
	Teardown() error
}
