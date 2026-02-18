package users

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
)

type UserIndexUpdater struct {
	bkt            objstore.InstrumentedBucket
	updateInterval time.Duration
	scanner        Scanner

	userIndexLastUpdated prometheus.Gauge
}

func NewUserIndexUpdater(bkt objstore.InstrumentedBucket, updateInterval time.Duration, scanner Scanner, reg prometheus.Registerer) *UserIndexUpdater {
	return &UserIndexUpdater{
		bkt:            bkt,
		updateInterval: updateInterval,
		scanner:        scanner,
		userIndexLastUpdated: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_user_index_last_successful_update_timestamp_seconds",
			Help: "Timestamp of the last successful update of user index.",
		}),
	}
}

func (u *UserIndexUpdater) GetUpdateInterval() time.Duration {
	return u.updateInterval
}

func (u *UserIndexUpdater) UpdateUserIndex(ctx context.Context) error {
	active, deleting, deleted, err := u.scanner.ScanUsers(ctx)
	if err != nil {
		return err
	}

	userIndex := &UserIndex{
		Version:       userIndexVersion,
		ActiveUsers:   active,
		DeletingUsers: deleting,
		DeletedUsers:  deleted,
		UpdatedAt:     time.Now().Unix(),
	}
	if err := WriteUserIndex(ctx, u.bkt, userIndex); err != nil {
		return err
	}
	u.userIndexLastUpdated.SetToCurrentTime()
	return nil
}
