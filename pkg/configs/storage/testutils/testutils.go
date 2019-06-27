package testutils

import (
	"github.com/cortexproject/cortex/pkg/configs"
)

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (configs.ConfigStore, error)
	Teardown() error
}
