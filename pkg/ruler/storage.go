package ruler

import (
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
)

// RuleStoreConfig conigures a rule store
type RuleStoreConfig struct {
	Type     string `yaml:"type"`
	ConfigDB client.Config

	mock *mockRuleStore
}

// RegisterFlags registers flags.
func (cfg *RuleStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("ruler.", f)
	f.StringVar(&cfg.Type, "ruler.storage.type", "configdb", "Method to use for backend rule storage (configdb, gcs)")
}

// NewRuleStorage returns a new rule storage backend poller and store
func NewRuleStorage(cfg RuleStoreConfig) (rules.RuleStore, error) {
	if cfg.mock != nil {
		return cfg.mock, nil
	}

	switch cfg.Type {
	case "configdb":
		return client.New(cfg.ConfigDB)
	default:
		return nil, fmt.Errorf("Unrecognized rule storage mode %v, choose one of: configdb, gcs", cfg.Type)
	}
}
