package alertmanager

import (
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/alertmanager/local"
	"github.com/cortexproject/cortex/pkg/configs/client"
)

// AlertStoreConfig configures the alertmanager backend
type AlertStoreConfig struct {
	Type     string            `yaml:"type"`
	ConfigDB client.Config     `yaml:"configdb"`
	Local    local.StoreConfig `yaml:"local"`
}

// RegisterFlags registers flags.
func (cfg *AlertStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Local.RegisterFlags(f)
	cfg.ConfigDB.RegisterFlagsWithPrefix("alertmanager.", f)
	f.StringVar(&cfg.Type, "alertmanager.storage.type", "configdb", "Method to use for backend alertmanager configs storage (configdb, file)")
}

// NewAlertStore returns a new rule storage backend poller and store
func NewAlertStore(cfg AlertStoreConfig) (alerts.AlertStore, error) {
	switch cfg.Type {
	case "configdb":
		c, err := client.New(cfg.ConfigDB)
		if err != nil {
			return nil, err
		}
		return alerts.NewConfigAlertStore(c), nil
	case "local":
		return local.NewStore(cfg.Local)
	default:
		return nil, fmt.Errorf("Unrecognized alertmanager storage mode %v, choose one of: configdb, local", cfg.Type)
	}
}
