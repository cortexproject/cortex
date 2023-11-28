package bucketindex

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
)

func TestBlockIDsFetcher_Fetch(t *testing.T) {
	t.Parallel()
	const userID = "user-1"

	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	ctx := context.Background()
	now := time.Now()
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	// Create a bucket index.
	block1 := &Block{ID: ulid.MustNew(1, nil)}
	block2 := &Block{ID: ulid.MustNew(2, nil)}
	block3 := &Block{ID: ulid.MustNew(3, nil)}
	mark1 := &BlockDeletionMark{ID: block1.ID, DeletionTime: now.Add(-time.Hour).Unix()}     // Below the ignore delay threshold.
	mark2 := &BlockDeletionMark{ID: block2.ID, DeletionTime: now.Add(-3 * time.Hour).Unix()} // Above the ignore delay threshold.

	require.NoError(t, WriteIndex(ctx, bkt, userID, nil, &Index{
		Version:            IndexVersion1,
		Blocks:             Blocks{block1, block2, block3},
		BlockDeletionMarks: BlockDeletionMarks{mark1, mark2},
		UpdatedAt:          now.Unix(),
	}))

	blockIdsFetcher := NewBlockIDsFetcher(logger, bkt, userID, nil)
	ch := make(chan ulid.ULID)
	var wg sync.WaitGroup
	var blockIds []ulid.ULID
	wg.Add(1)
	go func() {
		defer wg.Done()
		for id := range ch {
			blockIds = append(blockIds, id)
		}
	}()
	blockIdsFetcher.GetActiveAndPartialBlockIDs(ctx, ch)
	close(ch)
	wg.Wait()
	require.Equal(t, []ulid.ULID{block1.ID, block2.ID, block3.ID}, blockIds)
}

func TestBlockIDsFetcherFetcher_Fetch_NoBucketIndex(t *testing.T) {
	t.Parallel()
	const userID = "user-1"

	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	ctx := context.Background()
	now := time.Now()
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	//prepare tenant bucket
	var meta1, meta2, meta3 metadata.Meta
	block1 := &Block{ID: ulid.MustNew(1, nil)}
	meta1.Version = 1
	meta1.ULID = block1.ID
	block2 := &Block{ID: ulid.MustNew(2, nil)}
	meta2.Version = 1
	meta2.ULID = block2.ID
	block3 := &Block{ID: ulid.MustNew(3, nil)}
	meta3.Version = 1
	meta3.ULID = block3.ID
	metas := []metadata.Meta{meta1, meta2, meta3}
	mark1 := &BlockDeletionMark{ID: block1.ID, DeletionTime: now.Add(-time.Hour).Unix()}     // Below the ignore delay threshold.
	mark2 := &BlockDeletionMark{ID: block2.ID, DeletionTime: now.Add(-3 * time.Hour).Unix()} // Above the ignore delay threshold.
	marks := []*BlockDeletionMark{mark1, mark2}
	var buf bytes.Buffer
	for _, meta := range metas {
		require.NoError(t, json.NewEncoder(&buf).Encode(&meta))
		require.NoError(t, bkt.Upload(ctx, path.Join(userID, meta.ULID.String(), metadata.MetaFilename), &buf))
	}
	for _, mark := range marks {
		require.NoError(t, json.NewEncoder(&buf).Encode(mark))
		require.NoError(t, bkt.Upload(ctx, path.Join(userID, mark.ID.String(), metadata.DeletionMarkFilename), &buf))
	}
	blockIdsFetcher := NewBlockIDsFetcher(logger, bkt, userID, nil)
	ch := make(chan ulid.ULID)
	var wg sync.WaitGroup
	var blockIds []ulid.ULID
	wg.Add(1)
	go func() {
		defer wg.Done()
		for id := range ch {
			blockIds = append(blockIds, id)
		}
	}()
	blockIdsFetcher.GetActiveAndPartialBlockIDs(ctx, ch)
	close(ch)
	wg.Wait()
	require.Equal(t, []ulid.ULID{block1.ID, block2.ID, block3.ID}, blockIds)
}
