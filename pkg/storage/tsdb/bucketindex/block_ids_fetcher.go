package bucketindex

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

type BlockIDsFetcher struct {
	logger              log.Logger
	bkt                 objstore.Bucket
	userID              string
	cfgProvider         bucket.TenantConfigProvider
	baseBlockIDsFetcher block.BlockIDsFetcher
}

func NewBlockIDsFetcher(logger log.Logger, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider) *BlockIDsFetcher {
	userBkt := bucket.NewUserBucketClient(userID, bkt, cfgProvider)
	baseBlockIDsFetcher := block.NewBaseBlockIDsFetcher(logger, userBkt)
	return &BlockIDsFetcher{
		logger:              logger,
		bkt:                 bkt,
		userID:              userID,
		cfgProvider:         cfgProvider,
		baseBlockIDsFetcher: baseBlockIDsFetcher,
	}
}

func (f *BlockIDsFetcher) GetActiveAndPartialBlockIDs(ctx context.Context, ch chan<- ulid.ULID) (partialBlocks map[ulid.ULID]bool, err error) {
	// Fetch the bucket index.
	idx, err := ReadIndex(ctx, f.bkt, f.userID, f.cfgProvider, f.logger)
	if errors.Is(err, ErrIndexNotFound) {
		// This is a legit case happening when the first blocks of a tenant have recently been uploaded by ingesters
		// and their bucket index has not been created yet.
		// Fallback to BaseBlockIDsFetcher.
		return f.baseBlockIDsFetcher.GetActiveAndPartialBlockIDs(ctx, ch)
	}
	if errors.Is(err, ErrIndexCorrupted) {
		// In case a single tenant bucket index is corrupted, we want to return empty active blocks and parital blocks, so skipping this compaction cycle
		level.Error(f.logger).Log("msg", "corrupted bucket index found", "user", f.userID, "err", err)
		// Fallback to BaseBlockIDsFetcher.
		return f.baseBlockIDsFetcher.GetActiveAndPartialBlockIDs(ctx, ch)
	}

	if errors.Is(err, bucket.ErrCustomerManagedKeyAccessDenied) {
		// stop the job and return the error
		// this error should be used to return Access Denied to the caller
		level.Error(f.logger).Log("msg", "bucket index key permission revoked", "user", f.userID, "err", err)
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	blocksMarkedForDeletion := idx.BlockDeletionMarks.GetULIDs()
	blocksMarkedForDeletionMap := make(map[ulid.ULID]struct{})
	for _, block := range blocksMarkedForDeletion {
		blocksMarkedForDeletionMap[block] = struct{}{}
	}
	// Sent the ids of blocks not marked for deletion
	for _, b := range idx.Blocks {
		if _, ok := blocksMarkedForDeletionMap[b.ID]; ok {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case ch <- b.ID:
		}
	}
	return nil, nil
}
