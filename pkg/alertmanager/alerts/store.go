package alerts

import (
	"context"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/client"
)

// AlertStore stores and configures users rule configs
type AlertStore interface {
	ListAlertConfigs(ctx context.Context) (map[string]AlertConfigDesc, error)
}

// ConfigAlertStore is a concrete implementation of RuleStore that sources rules from the config service
type ConfigAlertStore struct {
	configClient client.Client
	since        configs.ID
	alertConfigs map[string]AlertConfigDesc
}

// NewConfigAlertStore constructs a ConfigAlertStore
func NewConfigAlertStore(c client.Client) *ConfigAlertStore {
	return &ConfigAlertStore{
		configClient: c,
		since:        0,
		alertConfigs: make(map[string]AlertConfigDesc),
	}
}

// ListAlertConfigs implements RuleStore
func (c *ConfigAlertStore) ListAlertConfigs(ctx context.Context) (map[string]AlertConfigDesc, error) {

	configs, err := c.configClient.GetAlerts(ctx, c.since)

	if err != nil {
		return nil, err
	}

	for user, cfg := range configs.Configs {
		if cfg.IsDeleted() {
			delete(c.alertConfigs, user)
			continue
		}

		var templates []*TemplateDesc
		for fn, template := range cfg.Config.TemplateFiles {
			templates = append(templates, &TemplateDesc{
				Filename: fn,
				Body:     template,
			})
		}

		c.alertConfigs[user] = AlertConfigDesc{
			User:      user,
			RawConfig: cfg.Config.AlertmanagerConfig,
			Templates: templates,
		}
	}

	c.since = configs.GetLatestConfigID()

	return c.alertConfigs, nil
}
