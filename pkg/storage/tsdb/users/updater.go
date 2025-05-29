package users

import (
	"context"
	"time"

	"github.com/thanos-io/objstore"
)

type UserIndexUpdater struct {
	bkt     objstore.InstrumentedBucket
	scanner Scanner
}

func NewUserIndexUpdater(bkt objstore.InstrumentedBucket, scanner Scanner) *UserIndexUpdater {
	return &UserIndexUpdater{
		bkt:     bkt,
		scanner: scanner,
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
