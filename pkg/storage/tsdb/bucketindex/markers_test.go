package bucketindex

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestBlockDeletionMarkFilepath(t *testing.T) {
	id := ulid.MustNew(1, nil)

	assert.Equal(t, "markers/"+id.String()+"-deletion-mark.json", BlockDeletionMarkFilepath(id))
}

func TestIsBlockDeletionMarkFilename(t *testing.T) {
	expected := ulid.MustNew(1, nil)

	_, ok := IsBlockDeletionMarkFilename("xxx")
	assert.False(t, ok)

	_, ok = IsBlockDeletionMarkFilename("xxx-deletion-mark.json")
	assert.False(t, ok)

	_, ok = IsBlockDeletionMarkFilename("tenant-deletion-mark.json")
	assert.False(t, ok)

	actual, ok := IsBlockDeletionMarkFilename(expected.String() + "-deletion-mark.json")
	assert.True(t, ok)
	assert.Equal(t, expected, actual)
}

func TestMigrateBlockDeletionMarksToGlobalLocation(t *testing.T) {
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	ctx := context.Background()

	// Create some fixtures.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", block1.String(), metadata.DeletionMarkFilename), strings.NewReader("{}")))
	require.NoError(t, bkt.Upload(ctx, path.Join("user-1", block3.String(), metadata.DeletionMarkFilename), strings.NewReader("{}")))

	require.NoError(t, MigrateBlockDeletionMarksToGlobalLocation(ctx, bkt, "user-1", nil))

	// Ensure deletion marks have been copied.
	for _, tc := range []struct {
		blockID        ulid.ULID
		expectedExists bool
	}{
		{block1, true},
		{block2, false},
		{block3, true},
	} {
		ok, err := bkt.Exists(ctx, path.Join("user-1", BlockDeletionMarkFilepath(tc.blockID)))
		require.NoError(t, err)
		require.Equal(t, tc.expectedExists, ok)
	}
}
