package ruler

import (
	"context"
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/ruler/store"
	"github.com/cortexproject/cortex/pkg/storage/clients/configdb"
	"github.com/cortexproject/cortex/pkg/storage/clients/gcp"
	"github.com/cortexproject/cortex/pkg/util/usertracker"
)

// RuleStoreConfig conigures a rule store
type RuleStoreConfig struct {
	Type     string `yaml:"type"`
	ConfigDB configdb.Config
	GCS      gcp.GCSConfig
	Tracker  usertracker.Config

	mock *mockRuleStore
}

// RegisterFlags registers flags.
func (cfg *RuleStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("ruler", f)
	cfg.GCS.RegisterFlagsWithPrefix("ruler.store.", f)
	cfg.Tracker.RegisterFlagsWithPrefix("ruler.", f)
	f.StringVar(&cfg.Type, "ruler.storage.type", "configdb", "Method to use for backend rule storage (configdb, gcs)")
}

// NewRuleStorage returns a new rule storage backend poller and store
func NewRuleStorage(cfg RuleStoreConfig) (store.RulePoller, store.RuleStore, error) {
	if cfg.mock != nil {
		return cfg.mock, cfg.mock, nil
	}

	var (
		ruleStore store.RuleStore
		err       error
	)
	switch cfg.Type {
	case "configdb":
		poller, err := configdb.New(cfg.ConfigDB)
		return poller, nil, err
	case "gcs":
		ruleStore, err = gcp.NewGCSClient(context.Background(), cfg.GCS)
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fmt.Errorf("Unrecognized rule storage mode %v, choose one of: configdb, gcs", cfg.Type)
	}

	tracker, err := usertracker.NewTracker(cfg.Tracker)
	if err != nil {
		return nil, nil, err
	}

	p, err := newTrackedPoller(tracker, ruleStore)

	return p, p.trackedRuleStore(), err
}
