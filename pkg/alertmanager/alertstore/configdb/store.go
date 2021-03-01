package configdb

import (
	"context"
	"errors"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/configs/userconfig"
)

const (
	Name = "configdb"
)

var (
	errReadOnly = errors.New("configdb alertmanager config storage is read-only")
)

// Store is a concrete implementation of RuleStore that sources rules from the config service
type Store struct {
	configClient client.Client
	since        userconfig.ID
	alertConfigs map[string]alertspb.AlertConfigDesc
}

// NewStore constructs a Store
func NewStore(c client.Client) *Store {
	return &Store{
		configClient: c,
		since:        0,
		alertConfigs: make(map[string]alertspb.AlertConfigDesc),
	}
}

// ListAlertConfigs implements alertstore.AlertStore.
func (c *Store) ListAlertConfigs(ctx context.Context) (map[string]alertspb.AlertConfigDesc, error) {

	configs, err := c.configClient.GetAlerts(ctx, c.since)

	if err != nil {
		return nil, err
	}

	for user, cfg := range configs.Configs {
		if cfg.IsDeleted() {
			delete(c.alertConfigs, user)
			continue
		}

		var templates []*alertspb.TemplateDesc
		for fn, template := range cfg.Config.TemplateFiles {
			templates = append(templates, &alertspb.TemplateDesc{
				Filename: fn,
				Body:     template,
			})
		}

		c.alertConfigs[user] = alertspb.AlertConfigDesc{
			User:      user,
			RawConfig: cfg.Config.AlertmanagerConfig,
			Templates: templates,
		}
	}

	c.since = configs.GetLatestConfigID()

	return c.alertConfigs, nil
}

// GetAlertConfig implements alertstore.AlertStore.
func (c *Store) GetAlertConfig(ctx context.Context, user string) (alertspb.AlertConfigDesc, error) {

	// Refresh the local state before fetching an specific one.
	_, err := c.ListAlertConfigs(ctx)
	if err != nil {
		return alertspb.AlertConfigDesc{}, err
	}

	cfg, exists := c.alertConfigs[user]

	if !exists {
		return alertspb.AlertConfigDesc{}, alertspb.ErrNotFound
	}

	return cfg, nil
}

// SetAlertConfig implements alertstore.AlertStore.
func (c *Store) SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error {
	return errReadOnly
}

// DeleteAlertConfig implements alertstore.AlertStore.
func (c *Store) DeleteAlertConfig(ctx context.Context, user string) error {
	return errReadOnly
}
