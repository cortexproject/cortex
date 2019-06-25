package storage

import (
	"context"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/weaveworks/common/instrument"
)

var configsRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "configs_request_duration_seconds",
	Help:      "Time spent requesting configs.",
	Buckets:   prometheus.DefBuckets,
}, []string{"operation", "status_code"}))

func init() {
	configsRequestDuration.Register()
}

type instrumented struct {
	next configs.ConfigStore
}

func (i instrumented) PollAlerts(ctx context.Context) (map[string]configs.AlertConfig, error) {
	var cfgs map[string]configs.AlertConfig
	err := instrument.CollectedRequest(context.Background(), "Configs.PollAlerts", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		cfgs, err = i.next.PollAlerts(ctx)
		return err
	})
	return cfgs, err
}

func (i instrumented) GetAlertConfig(ctx context.Context, userID string) (configs.AlertConfig, error) {
	var cfg configs.AlertConfig
	err := instrument.CollectedRequest(context.Background(), "Configs.GetAlertConfig", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		cfg, err = i.next.GetAlertConfig(ctx, userID)
		return err
	})
	return cfg, err
}

func (i instrumented) SetAlertConfig(ctx context.Context, userID string, config configs.AlertConfig) error {
	return instrument.CollectedRequest(context.Background(), "Configs.SetAlertConfig", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		err = i.next.SetAlertConfig(ctx, userID, config)
		return err
	})
}

func (i instrumented) DeleteAlertConfig(ctx context.Context, userID string) error {
	return instrument.CollectedRequest(context.Background(), "Configs.DeleteAlertConfig", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		err = i.next.DeleteAlertConfig(ctx, userID)
		return err
	})
}

func (i instrumented) PollRules(ctx context.Context) (map[string][]configs.RuleGroup, error) {
	var cfgs map[string][]configs.RuleGroup
	err := instrument.CollectedRequest(context.Background(), "Configs.PollRules", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		cfgs, err = i.next.PollRules(ctx)
		return err
	})

	return cfgs, err
}

func (i instrumented) ListRuleGroups(ctx context.Context, options configs.RuleStoreConditions) ([]configs.RuleNamespace, error) {
	var cfgs []configs.RuleNamespace
	err := instrument.CollectedRequest(context.Background(), "Configs.ListRuleGroups", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		cfgs, err = i.next.ListRuleGroups(ctx, options)
		return err
	})
	return cfgs, err
}

func (i instrumented) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (rulefmt.RuleGroup, error) {
	var cfg rulefmt.RuleGroup
	err := instrument.CollectedRequest(context.Background(), "Configs.GetRuleGroup", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		cfg, err = i.next.GetRuleGroup(ctx, userID, namespace, group)
		return err
	})
	return cfg, err
}

func (i instrumented) SetRuleGroup(ctx context.Context, userID string, namespace string, group rulefmt.RuleGroup) error {
	return instrument.CollectedRequest(context.Background(), "Configs.SetRuleGroup", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		err = i.next.SetRuleGroup(ctx, userID, namespace, group)
		return err
	})
}

func (i instrumented) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	return instrument.CollectedRequest(context.Background(), "Configs.DeleteRuleGroup", configsRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		err = i.next.DeleteRuleGroup(ctx, userID, namespace, group)
		return err
	})
}
