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

func (t traced) GetConfig(orgID configs.OrgID) (cfg configs.ConfigView, err error) {
	defer func() { t.trace("GetConfig", orgID, cfg, err) }()
	return t.d.GetConfig(orgID)
}

func (t traced) SetConfig(orgID configs.OrgID, cfg configs.Config) (err error) {
	defer func() { t.trace("SetConfig", orgID, cfg, err) }()
	return t.d.SetConfig(orgID, cfg)
}

func (t traced) GetAllConfigs() (cfgs map[configs.OrgID]configs.ConfigView, err error) {
	defer func() { t.trace("GetAllConfigs", cfgs, err) }()
	return t.d.GetAllConfigs()
}

func (t traced) GetConfigs(since configs.ID) (cfgs map[configs.OrgID]configs.ConfigView, err error) {
	defer func() { t.trace("GetConfigs", since, cfgs, err) }()
	return t.d.GetConfigs(since)
}

func (t traced) Close() (err error) {
	defer func() { t.trace("Close", err) }()
	return t.d.Close()
}
