package alertmanager

import (
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/alertmanager/storage/configdb"
	"github.com/cortexproject/cortex/pkg/alertmanager/storage/local"
)

// AlertStoreConfig configures the alertmanager backend
type AlertStoreConfig struct {
	Type     string `yaml:"type"`
	ConfigDB configdb.Config
	File     local.FileAlertStoreConfig `yaml:"file_config"`
}

// RegisterFlags registers flags.
func (cfg *AlertStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.File.RegisterFlags(f)
	cfg.ConfigDB.RegisterFlags(f)
	f.StringVar(&cfg.Type, "alertmanager.storage.type", "configdb", "Method to use for backend rule storage (configdb, file)")
}

// NewAlertStore returns a new rule storage backend poller and store
func NewAlertStore(cfg AlertStoreConfig) (AlertStore, error) {
	switch cfg.Type {
	case "configdb":
		return configdb.New(cfg.ConfigDB)
	case "file":
		return local.NewFileAlertStore(cfg.File)
	default:
		return nil, fmt.Errorf("Unrecognized rule storage mode %v, choose one of: configdb, file", cfg.Type)
	}
}
