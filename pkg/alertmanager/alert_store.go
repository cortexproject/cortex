package alertmanager

import (
	"context"
)

// AlertConfig is used to configure user alert managers
type AlertConfig struct {
	TemplateFiles      map[string]string `json:"template_files"`
	AlertmanagerConfig string            `json:"alertmanager_config"`
}

// AlertPoller polls for updated alerts
type AlertPoller interface {
	PollAlertConfigs(ctx context.Context) (map[string]AlertConfig, error)
	AlertStore() AlertStore
}

// AlertStore stores config information and template files to configure alertmanager tenants
type AlertStore interface {
	GetAlertConfig(ctx context.Context, id string) (AlertConfig, error)
	SetAlertConfig(ctx context.Context, id string, cfg AlertConfig) error
	DeleteAlertConfig(ctx context.Context, id string) error
}
