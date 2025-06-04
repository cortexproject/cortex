package users

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"

	"github.com/thanos-io/objstore"
)

type UserIndexUpdater struct {
	bkt     objstore.InstrumentedBucket
	scanner Scanner

	userIndexLastUpdated prometheus.Gauge
}

func NewUserIndexUpdater(bkt objstore.InstrumentedBucket, scanner Scanner, reg prometheus.Registerer) *UserIndexUpdater {
	return &UserIndexUpdater{
		bkt:     bkt,
		scanner: scanner,
		userIndexLastUpdated: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_user_index_last_successful_update_timestamp_seconds",
			Help: "Timestamp of the last successful update of user index.",
		}),
	}
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
	return WriteUserIndex(ctx, u.bkt, userIndex)
}
