package alertstore

import (
	"context"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
)

// AlertStore stores and configures users rule configs
type AlertStore interface {
	// ListAlertConfigs loads and returns the alertmanager configuration for all users.
	ListAlertConfigs(ctx context.Context) (map[string]alertspb.AlertConfigDesc, error)

	// GetAlertConfig loads and returns the alertmanager configuration for the given user.
	GetAlertConfig(ctx context.Context, user string) (alertspb.AlertConfigDesc, error)

	// SetAlertConfig stores the alertmanager configuration for an user.
	SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error

	// DeleteAlertConfig deletes the alertmanager configuration for an user.
	DeleteAlertConfig(ctx context.Context, user string) error
}
