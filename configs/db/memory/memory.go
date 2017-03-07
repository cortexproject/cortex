package memory

import (
	"database/sql"

	"github.com/weaveworks/cortex/configs"
)

type config struct {
	cfg configs.Config
	id  configs.ID
}

func (c config) toView() configs.ConfigView {
	return configs.ConfigView{
		ID:     c.id,
		Config: c.cfg,
	}
}

// DB is an in-memory database for testing, and local development
type DB struct {
	userCfgs map[configs.UserID]map[configs.Subsystem]config
	orgCfgs  map[configs.OrgID]map[configs.Subsystem]config
	id       uint
}

// New creates a new in-memory database
func New(_, _ string) (*DB, error) {
	return &DB{
		userCfgs: map[configs.UserID]map[configs.Subsystem]config{},
		orgCfgs:  map[configs.OrgID]map[configs.Subsystem]config{},
		id:       0,
	}, nil
}

// GetUserConfig gets the user's configuration.
func (d *DB) GetUserConfig(userID configs.UserID, subsystem configs.Subsystem) (configs.ConfigView, error) {
	c, ok := d.userCfgs[userID][subsystem]
	if !ok {
		return configs.ConfigView{}, sql.ErrNoRows
	}
	return c.toView(), nil
}

// SetUserConfig sets configuration for a user.
func (d *DB) SetUserConfig(userID configs.UserID, subsystem configs.Subsystem, cfg configs.Config) error {
	// XXX: Is this really how you assign a thing to a nested map?
	user, ok := d.userCfgs[userID]
	if !ok {
		user = map[configs.Subsystem]config{}
	}
	d.id++
	user[subsystem] = config{cfg: cfg, id: configs.ID(d.id)}
	d.userCfgs[userID] = user
	return nil
}

// GetOrgConfig gets the org's configuration.
func (d *DB) GetOrgConfig(orgID configs.OrgID, subsystem configs.Subsystem) (configs.ConfigView, error) {
	c, ok := d.orgCfgs[orgID][subsystem]
	if !ok {
		return configs.ConfigView{}, sql.ErrNoRows
	}
	return c.toView(), nil
}

// SetOrgConfig sets configuration for a org.
func (d *DB) SetOrgConfig(orgID configs.OrgID, subsystem configs.Subsystem, cfg configs.Config) error {
	// XXX: Is this really how you assign a thing to a nested map?
	org, ok := d.orgCfgs[orgID]
	if !ok {
		org = map[configs.Subsystem]config{}
	}
	d.id++
	org[subsystem] = config{cfg: cfg, id: configs.ID(d.id)}
	d.orgCfgs[orgID] = org
	return nil
}

// GetAllOrgConfigs gets all of the organization configs for a subsystem.
func (d *DB) GetAllOrgConfigs(subsystem configs.Subsystem) (map[configs.OrgID]configs.ConfigView, error) {
	cfgs := map[configs.OrgID]configs.ConfigView{}
	for org, subsystems := range d.orgCfgs {
		c, ok := subsystems[subsystem]
		if ok {
			cfgs[org] = c.toView()
		}
	}
	return cfgs, nil
}

// GetOrgConfigs gets all of the organization configs for a subsystem that
// have changed recently.
func (d *DB) GetOrgConfigs(subsystem configs.Subsystem, since configs.ID) (map[configs.OrgID]configs.ConfigView, error) {
	cfgs := map[configs.OrgID]configs.ConfigView{}
	for org, subsystems := range d.orgCfgs {
		c, ok := subsystems[subsystem]
		if ok && c.id > since {
			cfgs[org] = c.toView()
		}
	}
	return cfgs, nil
}

// GetAllUserConfigs gets all of the user configs for a subsystem.
func (d *DB) GetAllUserConfigs(subsystem configs.Subsystem) (map[configs.UserID]configs.ConfigView, error) {
	cfgs := map[configs.UserID]configs.ConfigView{}
	for user, subsystems := range d.userCfgs {
		c, ok := subsystems[subsystem]
		if ok {
			cfgs[user] = c.toView()
		}
	}
	return cfgs, nil
}

// GetUserConfigs gets all of the user configs for a subsystem that have
// changed recently.
func (d *DB) GetUserConfigs(subsystem configs.Subsystem, since configs.ID) (map[configs.UserID]configs.ConfigView, error) {
	cfgs := map[configs.UserID]configs.ConfigView{}
	for user, subsystems := range d.userCfgs {
		c, ok := subsystems[subsystem]
		if ok && c.id > since {
			cfgs[user] = c.toView()
		}
	}
	return cfgs, nil
}

// Close finishes using the db. Noop.
func (d *DB) Close() error {
	return nil
}
