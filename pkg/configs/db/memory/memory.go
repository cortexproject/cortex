package memory

import (
	"database/sql"

	"github.com/weaveworks/cortex/pkg/configs"
)

// DB is an in-memory database for testing, and local development
type DB struct {
	cfgs map[string]configs.View
	id   uint
}

// New creates a new in-memory database
func New(_, _ string) (*DB, error) {
	return &DB{
		cfgs: map[string]configs.View{},
		id:   0,
	}, nil
}

// GetConfig gets the user's configuration.
func (d *DB) GetConfig(userID string) (configs.View, error) {
	c, ok := d.cfgs[userID]
	if !ok {
		return configs.View{}, sql.ErrNoRows
	}
	return c, nil
}

// SetConfig sets configuration for a user.
func (d *DB) SetConfig(userID string, cfg configs.Config) error {
	d.cfgs[userID] = configs.View{Config: cfg, ID: configs.ID(d.id)}
	d.id++
	return nil
}

// GetAllConfigs gets all of the configs.
func (d *DB) GetAllConfigs() (map[string]configs.View, error) {
	return d.cfgs, nil
}

// GetConfigs gets all of the configs that have changed recently.
func (d *DB) GetConfigs(since configs.ID) (map[string]configs.View, error) {
	cfgs := map[string]configs.View{}
	for user, c := range d.cfgs {
		if c.ID > since {
			cfgs[user] = c
		}
	}
	return cfgs, nil
}

// Close finishes using the db. Noop.
func (d *DB) Close() error {
	return nil
}

// GetRulesConfig gets the rules config for a user.
func (d *DB) GetRulesConfig(userID string) (configs.VersionedRulesConfig, error) {
	c, ok := d.cfgs[userID]
	if !ok {
		return configs.VersionedRulesConfig{}, sql.ErrNoRows
	}
	return c.GetVersionedRulesConfig(), nil
}

// SetRulesConfig sets the rules config for a user.
func (d *DB) SetRulesConfig(userID string, config configs.RulesConfig) error {
	c, ok := d.cfgs[userID]
	if !ok {
		return d.SetConfig(userID, configs.Config{RulesFiles: config})
	}
	return d.SetConfig(userID, configs.Config{
		AlertmanagerConfig: c.Config.AlertmanagerConfig,
		RulesFiles:         config,
	})
}

// GetAllRulesConfigs gets the rules configs for all users that have them.
func (d *DB) GetAllRulesConfigs() (map[string]configs.VersionedRulesConfig, error) {
	cfgs := map[string]configs.VersionedRulesConfig{}
	for user, c := range d.cfgs {
		cfgs[user] = c.GetVersionedRulesConfig()
	}
	return cfgs, nil
}

// GetRulesConfigs gets the rules configs that have changed
// since the given config version.
func (d *DB) GetRulesConfigs(since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	cfgs := map[string]configs.VersionedRulesConfig{}
	for user, c := range d.cfgs {
		if c.ID <= since {
			continue
		}
		cfgs[user] = c.GetVersionedRulesConfig()
	}
	return cfgs, nil
}
