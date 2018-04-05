package memory

import (
	"database/sql"
	"time"

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

// SetDeletedAtConfig sets a deletedAt for configuration
// by adding a single new row with deleted_at set
// the same as SetConfig is actually insert
func (d *DB) SetDeletedAtConfig(userID string, deletedAt time.Time) error {
	cv, err := d.GetConfig(userID)
	if err != nil {
		return err
	}
	cv.DeletedAt = deletedAt
	cv.ID = configs.ID(d.id)
	d.cfgs[userID] = cv
	d.id++
	return nil
}

// DeactivateConfig deactivates configuration for a user by creating new configuration with DeletedAt set to now
func (d *DB) DeactivateConfig(userID string) error {
	return d.SetDeletedAtConfig(userID, time.Now())
}

// RestoreConfig restores deactivated configuration for a user by creating new configuration with empty DeletedAt
func (d *DB) RestoreConfig(userID string) error {
	return d.SetDeletedAtConfig(userID, time.Time{})
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
	cfg := c.GetVersionedRulesConfig()
	if cfg == nil {
		return configs.VersionedRulesConfig{}, sql.ErrNoRows
	}
	return *cfg, nil
}

// SetRulesConfig sets the rules config for a user.
func (d *DB) SetRulesConfig(userID string, oldConfig, newConfig *configs.RulesConfig) (bool, error) {
	c, ok := d.cfgs[userID]
	if !ok {
		return true, d.SetConfig(userID, configs.Config{RulesConfig: newConfig})
	}
	if !oldConfig.Equal(c.Config.RulesConfig) {
		return false, nil
	}
	return true, d.SetConfig(userID, configs.Config{
		AlertmanagerConfig: c.Config.AlertmanagerConfig,
		RulesConfig:        newConfig,
	})
}

// GetAllRulesConfigs gets the rules configs for all users that have them.
func (d *DB) GetAllRulesConfigs() (map[string]configs.VersionedRulesConfig, error) {
	cfgs := map[string]configs.VersionedRulesConfig{}
	for user, c := range d.cfgs {
		cfg := c.GetVersionedRulesConfig()
		if cfg != nil {
			cfgs[user] = *cfg
		}
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
		cfg := c.GetVersionedRulesConfig()
		if cfg != nil {
			cfgs[user] = *cfg
		}
	}
	return cfgs, nil
}
