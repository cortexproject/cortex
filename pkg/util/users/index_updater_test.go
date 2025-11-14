package users

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/testutil"
)

type mockScanner struct {
	active   []string
	deleting []string
	deleted  []string
	err      error
	calls    int
}

func (m *mockScanner) ScanUsers(ctx context.Context) (active, deleting, deleted []string, err error) {
	m.calls++
	return m.active, m.deleting, m.deleted, m.err
}

func TestUserIndexUpdater_UpdateUserIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := map[string]struct {
		scanner     *mockScanner
		expectedIdx *UserIndex
		expectErr   bool
	}{
		"successful update": {
			scanner: &mockScanner{
				active:   []string{"user-1", "user-2"},
				deleting: []string{"user-3"},
				deleted:  []string{"user-4"},
			},
			expectedIdx: &UserIndex{
				Version:       userIndexVersion,
				ActiveUsers:   []string{"user-1", "user-2"},
				DeletingUsers: []string{"user-3"},
				DeletedUsers:  []string{"user-4"},
			},
			expectErr: false,
		},
		"scanner error": {
			scanner: &mockScanner{
				err: assert.AnError,
			},
			expectErr: true,
		},
		"empty lists": {
			scanner: &mockScanner{
				active:   []string{},
				deleting: []string{},
				deleted:  []string{},
			},
			expectedIdx: &UserIndex{
				Version:       userIndexVersion,
				ActiveUsers:   []string{},
				DeletingUsers: []string{},
				DeletedUsers:  []string{},
			},
			expectErr: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

			updater := NewUserIndexUpdater(bkt, defaultCleanUpInterval, testData.scanner, nil)
			err := updater.UpdateUserIndex(ctx)

			if testData.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Read back the index and verify its contents
			readIdx, err := ReadUserIndex(ctx, bkt, log.NewNopLogger())
			require.NoError(t, err)
			require.NotNil(t, readIdx)

			// Verify the index contents
			assert.Equal(t, testData.expectedIdx.Version, readIdx.Version)
			assert.Equal(t, testData.expectedIdx.ActiveUsers, readIdx.ActiveUsers)
			assert.Equal(t, testData.expectedIdx.DeletingUsers, readIdx.DeletingUsers)
			assert.Equal(t, testData.expectedIdx.DeletedUsers, readIdx.DeletedUsers)
			assert.WithinDuration(t, time.Now(), readIdx.GetUpdatedAt(), 5*time.Second)
		})
	}
}

func TestUserIndexUpdater_UpdateUserIndex_WriteError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bkt := &bucket.ClientMock{}

	// Mock the scanner to return valid data
	scanner := &mockScanner{
		active:   []string{"user-1"},
		deleting: []string{},
		deleted:  []string{},
	}

	// Mock the bucket to return an error on upload
	bkt.MockUpload(UserIndexCompressedFilename, assert.AnError)

	updater := NewUserIndexUpdater(bkt, defaultCleanUpInterval, scanner, nil)
	err := updater.UpdateUserIndex(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload user index")
}
