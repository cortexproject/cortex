package alertstore

import (
	"context"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/bucketclient"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/configdb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/local"
	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

// AlertStore stores and configures users rule configs
type AlertStore interface {
	// ListAllUsers returns all users with alertmanager configuration.
	ListAllUsers(ctx context.Context) ([]string, error)

	// GetAlertConfigs loads and returns the alertmanager configuration for given users.
	// If any of the provided users has no configuration, then this function does not return an
	// error but the returned configs will not include the missing users.
	GetAlertConfigs(ctx context.Context, userIDs []string) (map[string]alertspb.AlertConfigDesc, error)

	// GetAlertConfig loads and returns the alertmanager configuration for the given user.
	GetAlertConfig(ctx context.Context, user string) (alertspb.AlertConfigDesc, error)

	// SetAlertConfig stores the alertmanager configuration for an user.
	SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error

	// DeleteAlertConfig deletes the alertmanager configuration for an user.
	// If configuration for the user doesn't exist, no error is reported.
	DeleteAlertConfig(ctx context.Context, user string) error

	// ListUsersWithFullState returns the list of users which have had state written.
	ListUsersWithFullState(ctx context.Context) ([]string, error)

	// GetFullState loads and returns the alertmanager state for the given user.
	GetFullState(ctx context.Context, user string) (alertspb.FullStateDesc, error)

	// SetFullState stores the alertmanager state for the given user.
	SetFullState(ctx context.Context, user string, fs alertspb.FullStateDesc) error

	// DeleteFullState deletes the alertmanager state for an user.
	// If state for the user doesn't exist, no error is reported.
	DeleteFullState(ctx context.Context, user string) error
}

// NewAlertStore returns a alertmanager store backend client based on the provided cfg.
func NewAlertStore(ctx context.Context, cfg Config, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer) (AlertStore, error) {
	if cfg.Backend == configdb.Name {
		c, err := client.New(cfg.ConfigDB)
		if err != nil {
			return nil, err
		}
		return configdb.NewStore(c), nil
	}

	if cfg.Backend == local.Name {
		return local.NewStore(cfg.Local)
	}

	bucketClient, err := bucket.NewClient(ctx, cfg.Config, "alertmanager-storage", logger, reg)
	if err != nil {
		return nil, err
	}

	return bucketclient.NewBucketAlertStore(bucketClient, cfgProvider, logger), nil
}
