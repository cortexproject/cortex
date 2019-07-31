package ruler

import (
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/configs/storage/clients/configdb"
	"github.com/cortexproject/cortex/pkg/configs/storage/rules"
)

// RuleStoreConfig conigures a rule store
type RuleStoreConfig struct {
	Type     string `yaml:"type"`
	ConfigDB configdb.Config

	mock *mockRuleStore
}

// RegisterFlags registers flags.
func (cfg *RuleStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("ruler", f)
	f.StringVar(&cfg.Type, "ruler.storage.type", "configdb", "Method to use for backend rule storage (configdb)")
}

// NewRuleStorage returns a new rule storage backend poller and store
func NewRuleStorage(cfg RuleStoreConfig) (rules.RulePoller, rules.RuleStore, error) {
	if cfg.mock != nil {
		return cfg.mock, cfg.mock, nil
	}

	switch cfg.Type {
	case "configdb":
		poller, err := configdb.New(cfg.ConfigDB)
		return poller, nil, err
	default:
		return nil, nil, fmt.Errorf("Unrecognized rule storage mode %v, choose one of: configdb, gcs", cfg.Type)
	}
}
