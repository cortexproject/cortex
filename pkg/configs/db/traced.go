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

func (t traced) DeactivateConfig(userID string) (err error) {
	defer func() { t.trace("DeactivateConfig", userID, err) }()
	return t.d.DeactivateConfig(userID)
}

func (t traced) RestoreConfig(userID string) (err error) {
	defer func() { t.trace("RestoreConfig", userID, err) }()
	return t.d.RestoreConfig(userID)
}

func (t traced) Close() (err error) {
	defer func() { t.trace("Close", err) }()
	return t.d.Close()
}

func (t traced) GetRulesConfig(userID string) (cfg configs.VersionedRulesConfig, err error) {
	defer func() { t.trace("GetRulesConfig", userID, cfg, err) }()
	return t.d.GetRulesConfig(userID)
}

func (t traced) SetRulesConfig(userID string, oldCfg, newCfg configs.RulesConfig) (updated bool, err error) {
	defer func() { t.trace("SetRulesConfig", userID, oldCfg, newCfg, updated, err) }()
	return t.d.SetRulesConfig(userID, oldCfg, newCfg)
}

func (t traced) GetAllRulesConfigs() (cfgs map[string]configs.VersionedRulesConfig, err error) {
	defer func() { t.trace("GetAllRulesConfigs", cfgs, err) }()
	return t.d.GetAllRulesConfigs()
}

func (t traced) GetRulesConfigs(since configs.ID) (cfgs map[string]configs.VersionedRulesConfig, err error) {
	defer func() { t.trace("GetConfigs", since, cfgs, err) }()
	return t.d.GetRulesConfigs(since)
}
