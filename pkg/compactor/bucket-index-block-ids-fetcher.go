package compactor

import (
	"context"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

type BucketIndexBlockIDsFetcher struct {
	logger      log.Logger
	bkt         objstore.Bucket
	userID      string
	cfgProvider bucket.TenantConfigProvider
}

func NewBucketIndexBlockIDsFetcher(logger log.Logger, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider) *BucketIndexBlockIDsFetcher {
	return &BucketIndexBlockIDsFetcher{
		logger:      logger,
		bkt:         bkt,
		userID:      userID,
		cfgProvider: cfgProvider,
	}
}

func (f *BucketIndexBlockIDsFetcher) GetActiveAndPartialBlockIDs(ctx context.Context, ch chan<- ulid.ULID) (partialBlocks map[ulid.ULID]bool, err error) {
	// Fetch the bucket index.
	idx, err := bucketindex.ReadIndex(ctx, f.bkt, f.userID, f.cfgProvider, f.logger)
	if errors.Is(err, bucketindex.ErrIndexNotFound) {
		// This is a legit case happening when the first blocks of a tenant have recently been uploaded by ingesters
		// and their bucket index has not been created yet.
		return nil, nil
	}
	if errors.Is(err, bucketindex.ErrIndexCorrupted) {
		// In case a single tenant bucket index is corrupted, we want to return empty active blocks and parital blocks, so skipping this compaction cycle
		level.Error(f.logger).Log("msg", "corrupted bucket index found", "user", f.userID, "err", err)
		return nil, nil
	}

	if errors.Is(err, bucket.ErrCustomerManagedKeyAccessDenied) {
		// stop the job and return the error
		// this error should be used to return Access Denied to the caller
		level.Error(f.logger).Log("msg", "bucket index key permission revoked", "user", f.userID, "err", err)
		return nil, err
	}

	if err != nil {
		return nil, errors.Wrapf(err, "read bucket index")
	}

	// Sent the active block ids
	for _, b := range idx.Blocks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case ch <- b.ID:
		}
	}
	return nil, nil
}
