package alertmanager

import (
	"context"
)

// AlertConfig is used to configure user alert managers
type AlertConfig struct {
	TemplateFiles      map[string]string `json:"template_files"`
	AlertmanagerConfig string            `json:"alertmanager_config"`
}

// AlertStore stores config information and template files to configure alertmanager tenants
type AlertStore interface {
	PollAlertConfigs(ctx context.Context) (map[string]AlertConfig, error)

	GetAlertConfig(ctx context.Context, id string) (AlertConfig, error)
	SetAlertConfig(ctx context.Context, id string, cfg AlertConfig) error
	DeleteAlertConfig(ctx context.Context, id string) error
}
