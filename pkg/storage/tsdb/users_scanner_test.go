package tsdb

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

func TestUsersScanner_ScanUsers_ShouldReturnedOwnedUsersOnly(t *testing.T) {
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1/", "user-2/", "user-3/", "user-4/"}, nil)
	bucketClient.MockIter("__markers__", []string{"__markers__/user-5/", "__markers__/user-6/", "__markers__/user-7/"}, nil)
	bucketClient.MockExists(GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(GetGlobalDeletionMarkPath("user-3"), true, nil)
	bucketClient.MockExists(GetLocalDeletionMarkPath("user-3"), false, nil)
	bucketClient.MockExists(GetGlobalDeletionMarkPath("user-7"), false, nil)
	bucketClient.MockExists(GetLocalDeletionMarkPath("user-7"), true, nil)

	isOwned := func(userID string) (bool, error) {
		return userID == "user-1" || userID == "user-3" || userID == "user-7", nil
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"user-1"}, actual)
	slices.Sort(deleted)
	assert.Equal(t, []string{"user-3", "user-7"}, deleted)
}

func TestUsersScanner_ScanUsers_ShouldReturnUsersForWhichOwnerCheckOrTenantDeletionCheckFailed(t *testing.T) {
	expected := []string{"user-1", "user-2"}

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", expected, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockExists(GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(GetLocalDeletionMarkPath("user-1"), false, nil)

	bucketClient.MockExists(GetGlobalDeletionMarkPath("user-2"), false, errors.New("fail"))

	isOwned := func(userID string) (bool, error) {
		return false, errors.New("failed to check if user is owned")
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	slices.Sort(actual)
	assert.Equal(t, expected, actual)
	assert.Empty(t, deleted)
}
