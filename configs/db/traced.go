package db

import (
	"github.com/Sirupsen/logrus"
	"github.com/weaveworks/cortex/configs"
)

// traced adds logrus trace lines on each db call
type traced struct {
	d DB
}

func (t traced) trace(name string, args ...interface{}) {
	logrus.Debugf("%s: %#v", name, args)
}

func (t traced) GetOrgConfig(orgID configs.OrgID) (cfg configs.ConfigView, err error) {
	defer func() { t.trace("GetOrgConfig", orgID, cfg, err) }()
	return t.d.GetOrgConfig(orgID)
}

func (t traced) SetOrgConfig(orgID configs.OrgID, cfg configs.Config) (err error) {
	defer func() { t.trace("SetOrgConfig", orgID, cfg, err) }()
	return t.d.SetOrgConfig(orgID, cfg)
}

func (t traced) GetAllOrgConfigs() (cfgs map[configs.OrgID]configs.ConfigView, err error) {
	defer func() { t.trace("GetAllOrgConfigs", cfgs, err) }()
	return t.d.GetAllOrgConfigs()
}

func (t traced) GetOrgConfigs(since configs.ID) (cfgs map[configs.OrgID]configs.ConfigView, err error) {
	defer func() { t.trace("GetOrgConfigs", since, cfgs, err) }()
	return t.d.GetOrgConfigs(since)
}

func (t traced) Close() (err error) {
	defer func() { t.trace("Close", err) }()
	return t.d.Close()
}
