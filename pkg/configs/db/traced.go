package db

import (
	"fmt"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/cortex/pkg/configs"
	"github.com/weaveworks/cortex/pkg/util"
)

// traced adds log trace lines on each db call
type traced struct {
	d DB
}

func (t traced) trace(name string, args ...interface{}) {
	level.Debug(util.Logger).Log("msg", fmt.Sprintf("%s: %#v", name, args))
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

func (t traced) GetAlertmanagerConfig(userID string) (cfg configs.VersionedAlertmanagerConfig, err error) {
	defer func() { t.trace("GetAlertmanagerConfig", userID, cfg, err) }()
	return t.d.GetAlertmanagerConfig(userID)
}

func (t traced) SetAlertmanagerConfig(userID string, cfg configs.AlertmanagerConfig) (err error) {
	defer func() { t.trace("SetAlertmanagerConfig", userID, cfg, err) }()
	return t.d.SetAlertmanagerConfig(userID, cfg)
}

func (t traced) GetAllAlertmanagerConfigs() (cfgs map[string]configs.VersionedAlertmanagerConfig, err error) {
	defer func() { t.trace("GetAllAlertmanagerConfigs", cfgs, err) }()
	return t.d.GetAllAlertmanagerConfigs()
}

func (t traced) GetAlertmanagerConfigs(since configs.ID) (cfgs map[string]configs.VersionedAlertmanagerConfig, err error) {
	defer func() { t.trace("GetConfigs", since, cfgs, err) }()
	return t.d.GetAlertmanagerConfigs(since)
}
