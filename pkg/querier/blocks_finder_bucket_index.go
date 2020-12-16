package querier

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/services"
)

var (
	errBucketIndexBlocksFinderNotRunning = errors.New("bucket index blocks finder is not running")
)

type BucketIndexBlocksFinderConfig struct {
	IndexUpdateInterval      time.Duration
	IndexIdleTimeout         time.Duration
	IgnoreDeletionMarksDelay time.Duration
}

// BucketIndexBlocksFinder implements BlocksFinder interface and find blocks in the bucket
// looking up the bucket index.
type BucketIndexBlocksFinder struct {
	services.Service

	cfg     BucketIndexBlocksFinderConfig
	manager *bucketindex.ReaderManager

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewBucketIndexBlocksFinder(cfg BucketIndexBlocksFinderConfig, bkt objstore.Bucket, logger log.Logger) (*BucketIndexBlocksFinder, error) {
	f := &BucketIndexBlocksFinder{
		cfg:     cfg,
		manager: bucketindex.NewReaderManager(bkt, logger, cfg.IndexUpdateInterval, cfg.IndexIdleTimeout),
	}

	var err error
	f.subservices, err = services.NewManager(f.manager)
	if err != nil {
		return nil, err
	}

	f.Service = services.NewBasicService(f.starting, f.running, f.stopping)

	return f, nil
}

func (f *BucketIndexBlocksFinder) starting(ctx context.Context) error {
	f.subservicesWatcher.WatchManager(f.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, f.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks index querier subservices")
	}

	return nil
}

func (f *BucketIndexBlocksFinder) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-f.subservicesWatcher.Chan():
			return errors.Wrap(err, "blocks undex querier set subservice failed")
		}
	}
}

func (f *BucketIndexBlocksFinder) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), f.subservices)
}

// GetBlocks implements BlocksFinder.
func (f *BucketIndexBlocksFinder) GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error) {
	if f.State() != services.Running {
		return nil, nil, errBucketIndexBlocksFinderNotRunning
	}
	if maxT < minT {
		return nil, nil, errInvalidBlocksRange
	}

	// Get the bucket index for this user.
	idx, err := f.manager.GetIndex(ctx, userID)
	if errors.Is(err, bucketindex.ErrIndexNotFound) {
		// This is a legit edge case, happening when a new tenant has not shipped blocks to the storage yet
		// so the bucket index hasn't been created yet.
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	var (
		matchingBlocks        = map[ulid.ULID]*bucketindex.Block{}
		matchingDeletionMarks = map[ulid.ULID]*bucketindex.BlockDeletionMark{}
	)

	// Filter blocks containing samples within the range.
	for _, block := range idx.Blocks {
		if !block.Within(minT, maxT) {
			continue
		}

		matchingBlocks[block.ID] = block
	}

	for _, mark := range idx.BlockDeletionMarks {
		// Filter deletion marks by matching blocks only.
		if _, ok := matchingBlocks[mark.ID]; !ok {
			continue
		}

		// Exclude blocks marked for deletion. This is the same logic as Thanos IgnoreDeletionMarkFilter.
		if time.Since(time.Unix(mark.DeletionTime, 0)).Seconds() > f.cfg.IgnoreDeletionMarksDelay.Seconds() {
			delete(matchingBlocks, mark.ID)
			continue
		}

		matchingDeletionMarks[mark.ID] = mark
	}

	// Convert matching blocks into a list.
	blocks := make(bucketindex.Blocks, 0, len(matchingBlocks))
	for _, b := range matchingBlocks {
		blocks = append(blocks, b)
	}

	return blocks, matchingDeletionMarks, nil
}
