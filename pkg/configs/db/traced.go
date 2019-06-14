package db

import (
	"context"
	"fmt"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// traced adds log trace lines on each db call
type traced struct {
	d DB
}

func (t traced) trace(name string, args ...interface{}) {
	level.Debug(util.Logger).Log("msg", fmt.Sprintf("%s: %#v", name, args))
}

func (t traced) GetConfig(ctx context.Context, userID string) (cfg configs.View, err error) {
	defer func() { t.trace("GetConfig", userID, cfg, err) }()
	return t.d.GetConfig(ctx, userID)
}

func (t traced) SetConfig(ctx context.Context, userID string, cfg configs.Config) (err error) {
	defer func() { t.trace("SetConfig", userID, cfg, err) }()
	return t.d.SetConfig(ctx, userID, cfg)
}

func (t traced) GetAllConfigs(ctx context.Context) (cfgs map[string]configs.View, err error) {
	defer func() { t.trace("GetAllConfigs", cfgs, err) }()
	return t.d.GetAllConfigs(ctx)
}

func (t traced) GetConfigs(ctx context.Context, since configs.ID) (cfgs map[string]configs.View, err error) {
	defer func() { t.trace("GetConfigs", since, cfgs, err) }()
	return t.d.GetConfigs(ctx, since)
}

func (t traced) DeactivateConfig(ctx context.Context, userID string) (err error) {
	defer func() { t.trace("DeactivateConfig", userID, err) }()
	return t.d.DeactivateConfig(ctx, userID)
}

func (t traced) RestoreConfig(ctx context.Context, userID string) (err error) {
	defer func() { t.trace("RestoreConfig", userID, err) }()
	return t.d.RestoreConfig(ctx, userID)
}

func (t traced) Close() (err error) {
	defer func() { t.trace("Close", err) }()
	return t.d.Close()
}

func (t traced) GetRulesConfig(ctx context.Context, userID string) (cfg configs.VersionedRulesConfig, err error) {
	defer func() { t.trace("GetRulesConfig", userID, cfg, err) }()
	return t.d.GetRulesConfig(ctx, userID)
}

func (t traced) SetRulesConfig(ctx context.Context, userID string, oldCfg, newCfg configs.RulesConfig) (updated bool, err error) {
	defer func() { t.trace("SetRulesConfig", userID, oldCfg, newCfg, updated, err) }()
	return t.d.SetRulesConfig(ctx, userID, oldCfg, newCfg)
}

func (t traced) GetAllRulesConfigs(ctx context.Context) (cfgs map[string]configs.VersionedRulesConfig, err error) {
	defer func() { t.trace("GetAllRulesConfigs", cfgs, err) }()
	return t.d.GetAllRulesConfigs(ctx)
}

func (t traced) GetRulesConfigs(ctx context.Context, since configs.ID) (cfgs map[string]configs.VersionedRulesConfig, err error) {
	defer func() { t.trace("GetConfigs", since, cfgs, err) }()
	return t.d.GetRulesConfigs(ctx, since)
}

func (t traced) GetRules(ctx context.Context) (cfgs map[string]configs.VersionedRulesConfig, err error) {
	defer func() { t.trace("GetRules", cfgs, err) }()
	return t.d.GetRules(ctx)
}

func (t traced) GetAlerts(ctx context.Context) (cfgs map[string]configs.View, err error) {
	defer func() { t.trace("GetAlerts", cfgs, err) }()
	return t.d.GetAlerts(ctx)
}
