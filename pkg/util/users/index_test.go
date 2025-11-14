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

func TestWriteAndReadUserIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Create a test index
	now := time.Now()
	idx := &UserIndex{
		Version:       userIndexVersion,
		UpdatedAt:     now.Unix(),
		ActiveUsers:   []string{"user-1", "user-2"},
		DeletingUsers: []string{"user-3"},
		DeletedUsers:  []string{"user-4"},
	}

	// Test writing the index
	err := WriteUserIndex(ctx, bkt, idx)
	require.NoError(t, err)

	// Verify the index was written with the correct filename
	_, err = bkt.Get(ctx, UserIndexFilename)
	require.Error(t, err)
	require.True(t, bkt.IsObjNotFoundErr(err))

	// Test reading the index
	readIdx, err := ReadUserIndex(ctx, bkt, log.NewNopLogger())
	require.NoError(t, err)
	require.NotNil(t, readIdx)

	// Verify the read index matches the written one
	assert.Equal(t, idx.Version, readIdx.Version)
	assert.Equal(t, idx.UpdatedAt, readIdx.UpdatedAt)
	assert.Equal(t, idx.ActiveUsers, readIdx.ActiveUsers)
	assert.Equal(t, idx.DeletingUsers, readIdx.DeletingUsers)
	assert.Equal(t, idx.DeletedUsers, readIdx.DeletedUsers)
}

func TestReadUserIndex_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Test reading non-existent index
	_, err := ReadUserIndex(ctx, bkt, log.NewNopLogger())
	require.Error(t, err)
	assert.Equal(t, ErrIndexNotFound, err)
}

func TestReadUserIndex_Corrupted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bkt := &bucket.ClientMock{}

	// Mock a corrupted index file
	bkt.MockGet(UserIndexCompressedFilename, "invalid gzip content", nil)

	// Test reading corrupted index
	_, err := ReadUserIndex(ctx, bkt, log.NewNopLogger())
	require.Error(t, err)
	assert.Equal(t, ErrIndexCorrupted, err)
}

func TestUserIndex_GetUpdatedAt(t *testing.T) {
	t.Parallel()

	now := time.Now()
	idx := &UserIndex{
		UpdatedAt: now.Unix(),
	}

	// Test GetUpdatedAt returns the correct time
	assert.Equal(t, now.Unix(), idx.GetUpdatedAt().Unix())
}

func TestWriteUserIndex_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bkt := &bucket.ClientMock{}

	// Mock upload error
	bkt.MockUpload(UserIndexCompressedFilename, assert.AnError)

	// Test writing index with error
	err := WriteUserIndex(ctx, bkt, &UserIndex{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload user index")
}
