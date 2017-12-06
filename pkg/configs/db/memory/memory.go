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

// GetAlertmanagerConfig gets the alertmanager config for a user.
func (d *DB) GetAlertmanagerConfig(userID string) (configs.VersionedAlertmanagerConfig, error) {
	c, ok := d.cfgs[userID]
	if !ok {
		return configs.VersionedAlertmanagerConfig{}, sql.ErrNoRows
	}
	return c.GetVersionedAlertmanagerConfig(), nil
}

// SetAlertmanagerConfig sets the alertmanager config for a user.
func (d *DB) SetAlertmanagerConfig(userID string, config configs.AlertmanagerConfig) error {
	c, ok := d.cfgs[userID]
	if !ok {
		return d.SetConfig(userID, configs.Config{AlertmanagerConfig: config})
	}
	return d.SetConfig(userID, configs.Config{
		RulesFiles:         c.Config.RulesFiles,
		AlertmanagerConfig: config,
	})
}

// GetAllAlertmanagerConfigs gets the alertmanager configs for all users that have them.
func (d *DB) GetAllAlertmanagerConfigs() (map[string]configs.VersionedAlertmanagerConfig, error) {
	cfgs := map[string]configs.VersionedAlertmanagerConfig{}
	for user, c := range d.cfgs {
		cfgs[user] = c.GetVersionedAlertmanagerConfig()
	}
	return cfgs, nil
}

// GetAlertmanagerConfigs gets the alertmanager configs that have changed
// since the given config version.
func (d *DB) GetAlertmanagerConfigs(since configs.ID) (map[string]configs.VersionedAlertmanagerConfig, error) {
	cfgs := map[string]configs.VersionedAlertmanagerConfig{}
	for user, c := range d.cfgs {
		if c.ID <= since {
			continue
		}
		cfgs[user] = c.GetVersionedAlertmanagerConfig()
	}
	return cfgs, nil
}
