package db

import (
	"flag"
	"fmt"
	"net/url"

	"github.com/weaveworks/cortex/configs"
	"github.com/weaveworks/cortex/configs/db/memory"
	"github.com/weaveworks/cortex/configs/db/postgres"
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

// DB is the interface for the database.
type DB interface {
	GetUserConfig(userID configs.UserID, subsystem configs.Subsystem) (configs.ConfigView, error)
	SetUserConfig(userID configs.UserID, subsystem configs.Subsystem, cfg configs.Config) error
	GetOrgConfig(orgID configs.OrgID, subsystem configs.Subsystem) (configs.ConfigView, error)
	SetOrgConfig(orgID configs.OrgID, subsystem configs.Subsystem, cfg configs.Config) error

	GetAllOrgConfigs(subsystem configs.Subsystem) (map[configs.OrgID]configs.ConfigView, error)
	GetOrgConfigs(subsystem configs.Subsystem, since configs.ID) (map[configs.OrgID]configs.ConfigView, error)
	GetAllUserConfigs(subsystem configs.Subsystem) (map[configs.UserID]configs.ConfigView, error)
	GetUserConfigs(subsystem configs.Subsystem, since configs.ID) (map[configs.UserID]configs.ConfigView, error)

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
