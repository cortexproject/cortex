package tsdb

import (
	"context"
	"strings"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"
)

// AllUsers returns true to each call and should be used whenever the UsersScanner should not filter out
// any user due to sharding.
func AllUsers(user string) (bool, error) {
	if user == util.GlobalMarkersDir {
		return false, nil
	}
	return true, nil
}

type UsersScanner struct {
	bucketClient objstore.Bucket
	logger       log.Logger
	isOwned      func(userID string) (bool, error)
}

func NewUsersScanner(bucketClient objstore.Bucket, isOwned func(userID string) (bool, error), logger log.Logger) *UsersScanner {
	return &UsersScanner{
		bucketClient: bucketClient,
		logger:       logger,
		isOwned:      isOwned,
	}
}

// ScanUsers returns a fresh list of users found in the storage, that are not marked for deletion,
// and list of users marked for deletion.
//
// If sharding is enabled, returned lists contains only the users owned by this instance.
func (s *UsersScanner) ScanUsers(ctx context.Context) (users, markedForDeletion []string, err error) {
	scannedUsers := make(map[string]struct{})

	// Scan users in the bucket.
	err = s.bucketClient.Iter(ctx, "", func(entry string) error {
		userID := strings.TrimSuffix(entry, "/")
		scannedUsers[userID] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	// Scan users from the __markers__ directory.
	err = s.bucketClient.Iter(ctx, util.GlobalMarkersDir, func(entry string) error {
		// entry will be of the form __markers__/<user>/
		parts := strings.Split(entry, objstore.DirDelim)
		userID := parts[1]
		scannedUsers[userID] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	for userID := range scannedUsers {
		// Filter out users not owned by this instance.
		if owned, err := s.isOwned(userID); err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
		} else if !owned {
			continue
		}

		// Filter users marked for deletion
		if deletionMarkExists, err := TenantDeletionMarkExists(ctx, s.bucketClient, userID); err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is marked for deletion", "user", userID, "err", err)
		} else if deletionMarkExists {
			markedForDeletion = append(markedForDeletion, userID)
			continue
		}

		// The remaining are the active users owned by this instance.
		users = append(users, userID)
	}

	return users, markedForDeletion, nil
}
