package users

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestListScanner_ScanUsers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := map[string]struct {
		bucketSetup      func(*bucket.ClientMock)
		expectedActive   []string
		expectedDeleting []string
		expectedDeleted  []string
		expectErr        bool
	}{
		"successful scan with all user types": {
			bucketSetup: func(b *bucket.ClientMock) {
				// Active users
				b.MockIter("", []string{"user-1/", "user-2/", "user-3/"}, nil)
				// Marked for deletion users
				b.MockIter("__markers__", []string{"__markers__/user-1/", "__markers__/user-4/", "__markers__/user-5/"}, nil)
				// Deletion marks
				b.MockExists(tsdb.GetGlobalDeletionMarkPath("user-1"), true, nil)
				b.MockExists(tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
				b.MockExists(tsdb.GetGlobalDeletionMarkPath("user-2"), false, nil)
				b.MockExists(tsdb.GetLocalDeletionMarkPath("user-2"), false, nil)
				b.MockExists(tsdb.GetGlobalDeletionMarkPath("user-3"), false, nil)
				b.MockExists(tsdb.GetLocalDeletionMarkPath("user-3"), false, nil)
			},
			expectedActive:   []string{"user-2", "user-3"},
			expectedDeleting: []string{"user-1"},
			expectedDeleted:  []string{"user-4", "user-5"},
			expectErr:        false,
		},
		"bucket iteration error": {
			bucketSetup: func(b *bucket.ClientMock) {
				b.MockIter("", nil, errors.New("failed to iterate bucket"))
			},
			expectErr: true,
		},
		"markers iteration error": {
			bucketSetup: func(b *bucket.ClientMock) {
				b.MockIter("", []string{"user-1/"}, nil)
				b.MockIter("__markers__", nil, errors.New("failed to iterate markers"))
			},
			expectErr: true,
		},
		"empty bucket": {
			bucketSetup: func(b *bucket.ClientMock) {
				b.MockIter("", []string{}, nil)
				b.MockIter("__markers__", []string{}, nil)
			},
			expectedActive:   []string{},
			expectedDeleting: []string{},
			expectedDeleted:  []string{},
			expectErr:        false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			bucketClient := &bucket.ClientMock{}
			testData.bucketSetup(bucketClient)

			scanner := &listScanner{bkt: bucketClient}
			active, deleting, deleted, err := scanner.ScanUsers(ctx)

			if testData.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expectedActive, active)
			assert.ElementsMatch(t, testData.expectedDeleting, deleting)
			assert.ElementsMatch(t, testData.expectedDeleted, deleted)
		})
	}
}

func TestUserIndexScanner_ScanUsers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()

	tests := map[string]struct {
		bucketSetup      func(*bucket.ClientMock)
		maxStalePeriod   time.Duration
		expectedActive   []string
		expectedDeleting []string
		expectedDeleted  []string
		expectErr        bool
	}{
		"successful scan from index": {
			bucketSetup: func(b *bucket.ClientMock) {
				index := &UserIndex{
					Version:       userIndexVersion,
					ActiveUsers:   []string{"user-1", "user-2"},
					DeletingUsers: []string{"user-3"},
					DeletedUsers:  []string{"user-4"},
					UpdatedAt:     time.Now().Unix(),
				}
				indexBytes, err := json.Marshal(index)
				require.NoError(t, err)

				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				_, err = gw.Write(indexBytes)
				require.NoError(t, err)
				require.NoError(t, gw.Close())

				b.MockGet(UserIndexCompressedFilename, buf.String(), nil)
			},
			maxStalePeriod:   1 * time.Hour,
			expectedActive:   []string{"user-1", "user-2"},
			expectedDeleting: []string{"user-3"},
			expectedDeleted:  []string{"user-4"},
			expectErr:        false,
		},
		"stale index falls back to base scanner": {
			bucketSetup: func(b *bucket.ClientMock) {
				// Return stale index
				index := &UserIndex{
					Version:       userIndexVersion,
					ActiveUsers:   []string{"user-1"},
					DeletingUsers: []string{},
					DeletedUsers:  []string{},
					UpdatedAt:     time.Now().Add(-2 * time.Hour).Unix(),
				}
				indexBytes, err := json.Marshal(index)
				require.NoError(t, err)

				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				_, err = gw.Write(indexBytes)
				require.NoError(t, err)
				require.NoError(t, gw.Close())

				b.MockGet(UserIndexCompressedFilename, buf.String(), nil)

				// Base scanner results
				b.MockIter("", []string{"user-2/"}, nil)
				b.MockIter("__markers__", []string{}, nil)
				b.MockExists(tsdb.GetGlobalDeletionMarkPath("user-2"), false, nil)
				b.MockExists(tsdb.GetLocalDeletionMarkPath("user-2"), false, nil)
			},
			maxStalePeriod:   1 * time.Hour,
			expectedActive:   []string{"user-2"},
			expectedDeleting: []string{},
			expectedDeleted:  []string{},
			expectErr:        false,
		},
		"index read error falls back to base scanner": {
			bucketSetup: func(b *bucket.ClientMock) {
				b.MockGet(UserIndexCompressedFilename, "", errors.New("failed to read index"))
				b.MockIter("", []string{"user-1/"}, nil)
				b.MockIter("__markers__", []string{}, nil)
				b.MockExists(tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
				b.MockExists(tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
			},
			maxStalePeriod:   1 * time.Hour,
			expectedActive:   []string{"user-1"},
			expectedDeleting: []string{},
			expectedDeleted:  []string{},
			expectErr:        false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			bucketClient := &bucket.ClientMock{}
			testData.bucketSetup(bucketClient)

			baseScanner := &listScanner{bkt: bucketClient}
			scanner := newUserIndexScanner(baseScanner, tsdb.UsersScannerConfig{
				MaxStalePeriod: testData.maxStalePeriod,
			}, bucketClient, logger, nil)

			active, deleting, deleted, err := scanner.ScanUsers(ctx)

			if testData.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expectedActive, active)
			assert.ElementsMatch(t, testData.expectedDeleting, deleting)
			assert.ElementsMatch(t, testData.expectedDeleted, deleted)
		})
	}
}

func TestShardedScanner_ScanUsers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()

	tests := map[string]struct {
		baseScanner      Scanner
		isOwned          func(string) (bool, error)
		expectedActive   []string
		expectedDeleting []string
		expectedDeleted  []string
		expectErr        bool
	}{
		"successful scan with ownership check": {
			baseScanner: &mockScanner{
				active:   []string{"user-1", "user-2", "user-3"},
				deleting: []string{"user-4", "user-5"},
				deleted:  []string{"user-6", "user-7"},
			},
			isOwned: func(userID string) (bool, error) {
				return userID == "user-1" || userID == "user-4" || userID == "user-6", nil
			},
			expectedActive:   []string{"user-1"},
			expectedDeleting: []string{"user-4"},
			expectedDeleted:  []string{"user-6"},
			expectErr:        false,
		},
		"ownership check error": {
			baseScanner: &mockScanner{
				active:   []string{"user-1"},
				deleting: []string{},
				deleted:  []string{},
			},
			isOwned: func(userID string) (bool, error) {
				return false, errors.New("failed to check ownership")
			},
			expectedActive:   []string{},
			expectedDeleting: []string{},
			expectedDeleted:  []string{},
			expectErr:        false,
		},
		"base scanner error": {
			baseScanner: &mockScanner{
				err: errors.New("base scanner error"),
			},
			isOwned: func(userID string) (bool, error) {
				return true, nil
			},
			expectErr: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			scanner := &shardedScanner{
				scanner: testData.baseScanner,
				isOwned: testData.isOwned,
				logger:  logger,
			}

			active, deleting, deleted, err := scanner.ScanUsers(ctx)

			if testData.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expectedActive, active)
			assert.ElementsMatch(t, testData.expectedDeleting, deleting)
			assert.ElementsMatch(t, testData.expectedDeleted, deleted)
		})
	}
}
