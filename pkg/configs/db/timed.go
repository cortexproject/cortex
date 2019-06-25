package db

import (
	"context"

	configs "github.com/cortexproject/cortex/pkg/configs/legacy_configs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
)

var (
	databaseRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "database_request_duration_seconds",
		Help:      "Time spent (in seconds) doing database requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "status_code"}))
)

func init() {
	databaseRequestDuration.Register()
}

// timed adds prometheus timings to another database implementation
type timed struct {
	d DB
}

func (t timed) errorCode(err error) string {
	switch err {
	case nil:
		return "200"
	default:
		return "500"
	}
}

func (t timed) GetConfig(ctx context.Context, userID string) (configs.View, error) {
	var cfg configs.View
	err := instrument.CollectedRequest(ctx, "DB.GetConfigs", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		cfg, err = t.d.GetConfig(ctx, userID) // Warning: this will produce an incorrect result if the configID ever overflows
		return err
	})
	return cfg, err
}

func (t timed) SetConfig(ctx context.Context, userID string, cfg configs.Config) error {
	return instrument.CollectedRequest(ctx, "DB.SetConfig", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		return t.d.SetConfig(ctx, userID, cfg) // Warning: this will produce an incorrect result if the configID ever overflows
	})
}

func (t timed) GetAllConfigs(ctx context.Context) (map[string]configs.View, error) {
	var (
		cfgs map[string]configs.View
		err  error
	)
	instrument.CollectedRequest(ctx, "DB.GetAllConfigs", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		cfgs, err = t.d.GetAllConfigs(ctx)
		return err
	})

	return cfgs, err
}

func (t timed) GetConfigs(ctx context.Context, since configs.ID) (map[string]configs.View, error) {
	var (
		cfgs map[string]configs.View
	)
	err := instrument.CollectedRequest(ctx, "DB.GetConfigs", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		cfgs, err = t.d.GetConfigs(ctx, since)
		return err
	})

	return cfgs, err
}

func (t timed) DeactivateConfig(ctx context.Context, userID string) error {
	return instrument.CollectedRequest(ctx, "DB.DeactivateConfig", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		return t.d.DeactivateConfig(ctx, userID)
	})
}

func (t timed) RestoreConfig(ctx context.Context, userID string) (err error) {
	return instrument.CollectedRequest(ctx, "DB.RestoreConfig", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		return t.d.RestoreConfig(ctx, userID)
	})
}

func (t timed) Close() error {
	return instrument.CollectedRequest(context.Background(), "DB.Close", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		return t.d.Close()
	})
}

func (t timed) GetRulesConfig(ctx context.Context, userID string) (configs.VersionedRulesConfig, error) {
	var cfg configs.VersionedRulesConfig
	err := instrument.CollectedRequest(ctx, "DB.GetRulesConfig", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		cfg, err = t.d.GetRulesConfig(ctx, userID)
		return err
	})

	return cfg, err
}

func (t timed) SetRulesConfig(ctx context.Context, userID string, oldCfg, newCfg configs.RulesConfig) (bool, error) {
	var updated bool
	err := instrument.CollectedRequest(ctx, "DB.SetRulesConfig", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		updated, err = t.d.SetRulesConfig(ctx, userID, oldCfg, newCfg)
		return err
	})

	return updated, err
}

func (t timed) GetAllRulesConfigs(ctx context.Context) (map[string]configs.VersionedRulesConfig, error) {
	var cfgs map[string]configs.VersionedRulesConfig
	err := instrument.CollectedRequest(ctx, "DB.GetAllRulesConfigs", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		cfgs, err = t.d.GetAllRulesConfigs(ctx)
		return err
	})

	return cfgs, err
}

func (t timed) GetRulesConfigs(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	var cfgs map[string]configs.VersionedRulesConfig
	err := instrument.CollectedRequest(ctx, "DB.GetRulesConfigs", databaseRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		cfgs, err = t.d.GetRulesConfigs(ctx, since)
		return err
	})

	return cfgs, err
}
