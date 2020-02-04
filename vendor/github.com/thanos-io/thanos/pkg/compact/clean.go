package compact

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
)

const (
	// PartialUploadThresholdAge is a time after partial block is assumed aborted and ready to be cleaned.
	// Keep it long as it is based on block creation time not upload start time.
	PartialUploadThresholdAge = 2 * 24 * time.Hour
)

func BestEffortCleanAbortedPartialUploads(ctx context.Context, logger log.Logger, fetcher block.MetadataFetcher, bkt objstore.Bucket, deleteAttempts prometheus.Counter) {
	level.Info(logger).Log("msg", "started cleaning of aborted partial uploads")
	_, partial, err := fetcher.Fetch(ctx)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to fetch metadata for cleaning of aborted partial uploads; skipping", "err", err)
	}

	// Delete partial blocks that are older than partialUploadThresholdAge.
	// TODO(bwplotka): This is can cause data loss if blocks are:
	// * being uploaded longer than partialUploadThresholdAge
	// * being uploaded and started after their partialUploadThresholdAge
	// can be assumed in this case. Keep partialUploadThresholdAge long for now.
	// Mitigate this by adding ModifiedTime to bkt and check that instead of ULID (block creation time).
	for id := range partial {
		if ulid.Now()-id.Time() <= uint64(PartialUploadThresholdAge/time.Millisecond) {
			// Minimum delay has not expired, ignore for now.
			continue
		}

		deleteAttempts.Inc()
		if err := block.Delete(ctx, logger, bkt, id); err != nil {
			level.Warn(logger).Log("msg", "failed to delete aborted partial upload; skipping", "block", id, "thresholdAge", PartialUploadThresholdAge, "err", err)
			return
		}
		level.Info(logger).Log("msg", "deleted aborted partial upload", "block", id, "thresholdAge", PartialUploadThresholdAge)
	}
	level.Info(logger).Log("msg", "cleaning of aborted partial uploads done")
}
