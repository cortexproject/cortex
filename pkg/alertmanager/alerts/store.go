package alerts

import (
	"context"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/client"
)

// AlertStore stores and configures users rule configs
type AlertStore interface {
	ListAlertConfigs(ctx context.Context) ([]alerts.AlertConfigDesc, error)
}

// ConfigAlertStore is a concrete implementation of RuleStore that sources rules from the config service
type ConfigAlertStore struct {
	configClient client.Client
	since        configs.ID
	alertConfigs map[string]alerts.AlertConfigDesc
}

// NewConfigAlertStore constructs a ConfigAlertStore
func NewConfigAlertStore(c client.Client) *ConfigAlertStore {
	return &ConfigAlertStore{
		configClient: c,
		since:        0,
		alertConfigs: make(map[string]alerts.AlertConfigDesc),
	}
}

// ListAlertConfigs implements RuleStore
func (c *ConfigAlertStore) ListAlertConfigs(ctx context.Context) (map[string]alerts.AlertConfigDesc, error) {

	configs, err := c.configClient.GetAlerts(ctx, c.since)

	if err != nil {
		return nil, err
	}

	for user, cfg := range configs.Configs {
		if cfg.IsDeleted() {
			delete(c.alertConfigs, user)
			continue
		}

		var templates []*alerts.TemplateDesc
		for fn, template := range cfg.Config.TemplateFiles {
			templates = append(templates, &alerts.TemplateDesc{
				Filename: fn,
				Body:     template,
			})
		}

		userRules := alerts.AlertConfigDesc{
			User:      user,
			RawConfig: cfg.Config.AlertmanagerConfig,
			Templates: templates,
		}

		c.alertConfigs[user] = userRules
	}

	c.since = configs.GetLatestConfigID()

	return c.alertConfigs, nil
}
