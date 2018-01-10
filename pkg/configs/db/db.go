package db

import (
	"flag"
	"fmt"
	"net/url"

	"github.com/weaveworks/cortex/pkg/configs"
	"github.com/weaveworks/cortex/pkg/configs/db/memory"
	"github.com/weaveworks/cortex/pkg/configs/db/postgres"
)

// Config configures the database.
type Config struct {
	URI           string
	MigrationsDir string
}

// RegisterFlags adds the flags required to configure this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.URI, "database.uri", "postgres://postgres@configs-db.weave.local/configs?sslmode=disable", "URI where the database can be found (for dev you can use memory://)")
	flag.StringVar(&cfg.MigrationsDir, "database.migrations", "", "Path where the database migration files can be found")
}

// RulesDB has ruler-specific DB interfaces.
type RulesDB interface {
	// GetRulesConfig gets the user's ruler config
	GetRulesConfig(userID string) (configs.VersionedRulesConfig, error)
	// SetRulesConfig sets the user's ruler config
	SetRulesConfig(userID string, oldConfig, newConfig configs.RulesConfig) (bool, error)

	// GetAllRulesConfigs gets all of the ruler configs
	GetAllRulesConfigs() (map[string]configs.VersionedRulesConfig, error)
	// GetRulesConfigs gets all of the configs that have been added or have
	// changed since the provided config.
	GetRulesConfigs(since configs.ID) (map[string]configs.VersionedRulesConfig, error)
}

// DB is the interface for the database.
type DB interface {
	RulesDB

	GetConfig(userID string) (configs.View, error)
	SetConfig(userID string, cfg configs.Config) error

	GetAllConfigs() (map[string]configs.View, error)
	GetConfigs(since configs.ID) (map[string]configs.View, error)

	Close() error
}

// New creates a new database.
func New(cfg Config) (DB, error) {
	u, err := url.Parse(cfg.URI)
	if err != nil {
		return nil, err
	}
	var d DB
	switch u.Scheme {
	case "memory":
		d, err = memory.New(cfg.URI, cfg.MigrationsDir)
	case "postgres":
		d, err = postgres.New(cfg.URI, cfg.MigrationsDir)
	default:
		return nil, fmt.Errorf("Unknown database type: %s", u.Scheme)
	}
	if err != nil {
		return nil, err
	}
	return traced{timed{d}}, nil
}

// NewRulesDB creates a new rules config database.
func NewRulesDB(cfg Config) (RulesDB, error) {
	db, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return db, err
}
