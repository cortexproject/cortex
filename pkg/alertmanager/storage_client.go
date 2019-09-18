package alertmanager

import (
	"context"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
)

// AlertStore stores and configures users rule configs
type AlertStore interface {
	ListAlertConfigs(ctx context.Context) ([]alerts.AlertConfigDesc, error)
}
