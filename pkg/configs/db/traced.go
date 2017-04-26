package db

import (
	"github.com/Sirupsen/logrus"
	"github.com/weaveworks/cortex/pkg/configs"
)

// traced adds logrus trace lines on each db call
type traced struct {
	d DB
}

func (t traced) trace(name string, args ...interface{}) {
	logrus.Debugf("%s: %#v", name, args)
}

func (t traced) GetConfig(userID string) (cfg configs.View, err error) {
	defer func() { t.trace("GetConfig", userID, cfg, err) }()
	return t.d.GetConfig(userID)
}

func (t traced) SetConfig(userID string, cfg configs.Config) (err error) {
	defer func() { t.trace("SetConfig", userID, cfg, err) }()
	return t.d.SetConfig(userID, cfg)
}

func (t traced) GetAllConfigs() (cfgs map[string]configs.View, err error) {
	defer func() { t.trace("GetAllConfigs", cfgs, err) }()
	return t.d.GetAllConfigs()
}

func (t traced) GetConfigs(since configs.ID) (cfgs map[string]configs.View, err error) {
	defer func() { t.trace("GetConfigs", since, cfgs, err) }()
	return t.d.GetConfigs(since)
}

func (t traced) Close() (err error) {
	defer func() { t.trace("Close", err) }()
	return t.d.Close()
}
