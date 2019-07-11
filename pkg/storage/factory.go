package storage

import (
	"context"
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/storage/clients/configdb"
	"github.com/cortexproject/cortex/pkg/storage/clients/gcp"
)

// Config is used to configure the alert and rule store config clients
type Config struct {
	AlertStoreConfig ClientConfig
	RuleStoreConfig  ClientConfig
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.AlertStoreConfig.RegisterFlagsWithPrefix("alertmanager", f)
	cfg.RuleStoreConfig.RegisterFlagsWithPrefix("ruler", f)
}

// ClientConfig is used to config an config client
type ClientConfig struct {
	BackendType string

	ConfigDB configdb.Config
	GCS      gcp.GCSConfig
}

// RegisterFlagsWithPrefix registers flags.
func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BackendType, prefix+".store.backend", "configdb", "backend to use for storing and retrieving alerts")
	cfg.ConfigDB.RegisterFlagsWithPrefix(prefix, f)
	cfg.GCS.RegisterFlagsWithPrefix(prefix+".store.", f)
}

// NewRuleStore returns a rule store
func NewRuleStore(cfg Config) (ruler.RuleStore, error) {
	var (
		store ruler.RuleStore
		err   error
	)
	switch cfg.RuleStoreConfig.BackendType {
	case "configdb":
		store, err = configdb.New(cfg.RuleStoreConfig.ConfigDB)
	case "gcp":
		store, err = gcp.NewGCSClient(context.Background(), cfg.RuleStoreConfig.GCS)
	default:
		return nil, fmt.Errorf("Unrecognized rule store client %v, choose one of: client, gcp", cfg.RuleStoreConfig.BackendType)
	}

	return store, err
}

// NewAlertStore returns a alert store
func NewAlertStore(cfg Config) (alertmanager.AlertStore, error) {
	var (
		store alertmanager.AlertStore
		err   error
	)
	switch cfg.AlertStoreConfig.BackendType {
	case "configdb":
		store, err = configdb.New(cfg.AlertStoreConfig.ConfigDB)
	case "gcp":
		store, err = gcp.NewGCSClient(context.Background(), cfg.AlertStoreConfig.GCS)
	default:
		return nil, fmt.Errorf("Unrecognized config storage client %v, choose one of: client, gcp", cfg.AlertStoreConfig.BackendType)
	}

	return store, err
}
