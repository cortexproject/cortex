package db

import (
	"context"
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/db/memory"
	"github.com/cortexproject/cortex/pkg/configs/db/postgres"
)

// Config configures the database.
type Config struct {
	Type     string             `yaml:"type"`
	PostGres postgres.Config    `yaml:"postgres,omitempty"`
	GCS      gcp.ConfigDBConfig `yaml:"gcs,omitempty"`

	// Allow injection of mock DBs for unit testing.
	Mock DB
}

// RegisterFlags adds the flags required to configure this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.Type, "configdb.type", "postgres", "Config database backend to utilize, (postgres, memory, gcp)")
	cfg.PostGres.RegisterFlags(f)
	cfg.GCS.RegisterFlags(f)
}

// DB is the interface for the database.
type DB interface {
	Poller

	// GetRulesConfig gets the user's ruler config
	GetRulesConfig(ctx context.Context, userID string) (configs.VersionedRulesConfig, error)

	// SetRulesConfig does a compare-and-swap (CAS) on the user's rules config.
	// `oldConfig` must precisely match the current config in order to change the config to `newConfig`.
	// Will return `true` if the config was updated, `false` otherwise.
	SetRulesConfig(ctx context.Context, userID string, oldConfig, newConfig configs.RulesConfig) (bool, error)

	// GetAllRulesConfigs gets all of the ruler configs
	GetAllRulesConfigs(ctx context.Context) (map[string]configs.VersionedRulesConfig, error)

	// GetRulesConfigs gets all of the configs that have been added or have
	// changed since the provided config.
	GetRulesConfigs(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error)

	GetConfig(ctx context.Context, userID string) (configs.View, error)
	SetConfig(ctx context.Context, userID string, cfg configs.Config) error

	GetAllConfigs(ctx context.Context) (map[string]configs.View, error)
	GetConfigs(ctx context.Context, since configs.ID) (map[string]configs.View, error)

	DeactivateConfig(ctx context.Context, userID string) error
	RestoreConfig(ctx context.Context, userID string) error

	Close() error
}

// Poller is the interface for getting recently updated rules from the database
type Poller interface {
	GetRules(ctx context.Context) (map[string]configs.VersionedRulesConfig, error)
	GetAlerts(ctx context.Context) (map[string]configs.View, error)
}

// New creates a new database.
func New(cfg Config) (DB, error) {
	if cfg.Mock != nil {
		return cfg.Mock, nil
	}

	var (
		d   DB
		err error
	)
	switch cfg.Type {
	case "memory":
		d, err = memory.New()
	case "postgres":
		d, err = postgres.New(cfg.PostGres)
	case "gcs":
		d, err = gcp.NewConfigClient(context.TODO(), cfg.GCS)
	default:
		return nil, fmt.Errorf("Unknown database type: %s", cfg.Type)
	}
	if err != nil {
		return nil, err
	}
	return traced{timed{d}}, nil
}
