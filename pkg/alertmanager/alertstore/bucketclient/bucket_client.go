package bucketclient

import (
	"context"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
)

const (
	// The bucket prefix under which all tenants alertmanager configs are stored.
	alertsPrefix = "alerts"

	// How many users to load concurrently.
	fetchConcurrency = 16
)

// BucketAlertStore is used to support the AlertStore interface against an object storage backend. It is implemented
// using the Thanos objstore.Bucket interface
type BucketAlertStore struct {
	store *ProtobufStore
}

func NewBucketAlertStore(bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *BucketAlertStore {
	return &BucketAlertStore{
		store: NewProtobufStore(alertsPrefix, bkt, cfgProvider, logger),
	}
}

// ListAllUsers implements alertstore.AlertStore.
func (s *BucketAlertStore) ListAllUsers(ctx context.Context) ([]string, error) {
	return s.store.ListAllUsers(ctx)
}

// GetAlertConfigs implements alertstore.AlertStore.
func (s *BucketAlertStore) GetAlertConfigs(ctx context.Context, userIDs []string) (map[string]alertspb.AlertConfigDesc, error) {
	var (
		cfgsMx = sync.Mutex{}
		cfgs   = make(map[string]alertspb.AlertConfigDesc, len(userIDs))
	)

	err := concurrency.ForEach(ctx, concurrency.CreateJobsFromStrings(userIDs), fetchConcurrency, func(ctx context.Context, job interface{}) error {
		userID := job.(string)

		cfg := alertspb.AlertConfigDesc{}
		err := s.store.Get(ctx, userID, &cfg)
		if s.store.IsNotFoundErr(err) {
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "failed to fetch alertmanager config for user %s", userID)
		}

		cfgsMx.Lock()
		cfgs[userID] = cfg
		cfgsMx.Unlock()

		return nil
	})

	return cfgs, err
}

// GetAlertConfig implements alertstore.AlertStore.
func (s *BucketAlertStore) GetAlertConfig(ctx context.Context, userID string) (alertspb.AlertConfigDesc, error) {
	cfg := alertspb.AlertConfigDesc{}
	err := s.store.Get(ctx, userID, &cfg)
	if s.store.IsNotFoundErr(err) {
		return cfg, alertspb.ErrNotFound
	}

	return cfg, err
}

// SetAlertConfig implements alertstore.AlertStore.
func (s *BucketAlertStore) SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error {
	return s.store.Set(ctx, cfg.User, &cfg)
}

// DeleteAlertConfig implements alertstore.AlertStore.
func (s *BucketAlertStore) DeleteAlertConfig(ctx context.Context, userID string) error {
	return s.store.Delete(ctx, userID)
}
