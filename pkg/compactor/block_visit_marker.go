package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/util/runutil"
)

const (
	// BlockVisitMarkerFileSuffix is the known suffix of json filename for representing the most recent compactor visit.
	BlockVisitMarkerFileSuffix = "visit-mark.json"
	// BlockVisitMarkerFilePrefix is the known prefix of json filename for representing the most recent compactor visit.
	BlockVisitMarkerFilePrefix = "partition-"
	// VisitMarkerVersion1 is the current supported version of visit-mark file.
	VisitMarkerVersion1 = 1
)

var (
	ErrorBlockVisitMarkerNotFound  = errors.New("block visit marker not found")
	ErrorUnmarshalBlockVisitMarker = errors.New("unmarshal block visit marker JSON")
	ErrorNotBlockVisitMarker       = errors.New("file is not block visit marker")
)

type VisitStatus string

const (
	Pending   VisitStatus = "pending"
	Completed VisitStatus = "completed"
)

type BlockVisitMarker struct {
	CompactorID        string      `json:"compactorID"`
	Status             VisitStatus `json:"status"`
	PartitionedGroupID uint32      `json:"partitionedGroupID"`
	PartitionID        int         `json:"partitionID"`
	// VisitTime is a unix timestamp of when the block was visited (mark updated).
	VisitTime int64 `json:"visitTime"`
	// Version of the file.
	Version int `json:"version"`
}

func (b *BlockVisitMarker) isVisited(blockVisitMarkerTimeout time.Duration, partitionID int) bool {
	return b.isCompleted() || partitionID == b.PartitionID && time.Now().Before(time.Unix(b.VisitTime, 0).Add(blockVisitMarkerTimeout))
}

func (b *BlockVisitMarker) isVisitedByCompactor(blockVisitMarkerTimeout time.Duration, partitionID int, compactorID string) bool {
	return b.CompactorID == compactorID && b.isVisited(blockVisitMarkerTimeout, partitionID)
}

func (b *BlockVisitMarker) isCompleted() bool {
	return b.Status == Completed
}

func GetBlockVisitMarkerFile(blockID string, partitionID int) string {
	return path.Join(blockID, fmt.Sprintf("%s%d-%s", BlockVisitMarkerFilePrefix, partitionID, BlockVisitMarkerFileSuffix))
}

func ReadBlockVisitMarker(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, blockID string, partitionID int, blockVisitMarkerReadFailed prometheus.Counter) (*BlockVisitMarker, error) {
	visitMarkerFile := GetBlockVisitMarkerFile(blockID, partitionID)
	visitMarkerFileReader, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, visitMarkerFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, errors.Wrapf(ErrorBlockVisitMarkerNotFound, "block visit marker file: %s", visitMarkerFile)
		}
		blockVisitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(err, "get block visit marker file: %s", visitMarkerFile)
	}
	defer runutil.CloseWithLogOnErr(logger, visitMarkerFileReader, "close block visit marker reader")
	b, err := io.ReadAll(visitMarkerFileReader)
	if err != nil {
		blockVisitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(err, "read block visit marker file: %s", visitMarkerFile)
	}
	blockVisitMarker := BlockVisitMarker{}
	if err = json.Unmarshal(b, &blockVisitMarker); err != nil {
		blockVisitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(ErrorUnmarshalBlockVisitMarker, "block visit marker file: %s, error: %v", visitMarkerFile, err.Error())
	}
	if blockVisitMarker.Version != VisitMarkerVersion1 {
		return nil, errors.Errorf("unexpected block visit mark file version %d, expected %d", blockVisitMarker.Version, VisitMarkerVersion1)
	}
	return &blockVisitMarker, nil
}

func UpdateBlockVisitMarker(ctx context.Context, bkt objstore.Bucket, blockID string, partitionID int, reader io.Reader, blockVisitMarkerWriteFailed prometheus.Counter) error {
	blockVisitMarkerFilePath := GetBlockVisitMarkerFile(blockID, partitionID)
	if err := bkt.Upload(ctx, blockVisitMarkerFilePath, reader); err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return err
	}
	return nil
}

func generateBlocksInfo(blocks []*metadata.Meta) string {
	var blockIds []string
	for _, block := range blocks {
		blockIds = append(blockIds, block.ULID.String())
	}
	return strings.Join(blockIds, ",")
}

func markBlocksVisited(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	blocks []*metadata.Meta,
	marker BlockVisitMarker,
	blockVisitMarkerWriteFailed prometheus.Counter,
) {
	visitMarkerFileContent, err := json.Marshal(marker)
	if err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return
	}
	reader := bytes.NewReader(visitMarkerFileContent)
	for _, block := range blocks {
		select {
		// Exit early if possible.
		case <-ctx.Done():
			return
		default:
		}

		blockID := block.ULID.String()
		if err := UpdateBlockVisitMarker(ctx, bkt, blockID, marker.PartitionID, reader, blockVisitMarkerWriteFailed); err != nil {
			level.Error(logger).Log("msg", "unable to upsert visit marker file content for block", "partition_id", marker.PartitionID, "block_id", blockID, "err", err)
		}
		reader.Reset(visitMarkerFileContent)
	}
	level.Debug(logger).Log("msg", "marked blocks visited", "partition_id", marker.PartitionID, "blocks", generateBlocksInfo(blocks))
}

func markBlocksVisitedHeartBeat(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	blocks []*metadata.Meta,
	partitionedGroupID uint32,
	partitionID int,
	compactorID string,
	blockVisitMarkerFileUpdateInterval time.Duration,
	blockVisitMarkerWriteFailed prometheus.Counter,
	errChan chan error,
) {
	blocksInfo := generateBlocksInfo(blocks)
	level.Info(logger).Log("msg", "start visit marker heart beat", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "blocks", blocksInfo)
	ticker := time.NewTicker(blockVisitMarkerFileUpdateInterval)
	defer ticker.Stop()
	isComplete := false
heartBeat:
	for {
		level.Debug(logger).Log("msg", fmt.Sprintf("heart beat for blocks: %s", blocksInfo))
		blockVisitMarker := BlockVisitMarker{
			VisitTime:          time.Now().Unix(),
			CompactorID:        compactorID,
			Status:             Pending,
			PartitionedGroupID: partitionedGroupID,
			PartitionID:        partitionID,
			Version:            VisitMarkerVersion1,
		}
		markBlocksVisited(ctx, bkt, logger, blocks, blockVisitMarker, blockVisitMarkerWriteFailed)

		select {
		case <-ctx.Done():
			level.Warn(logger).Log("msg", "visit marker heart beat got cancelled", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "blocks", blocksInfo)
			break heartBeat
		case <-ticker.C:
			continue
		case err := <-errChan:
			isComplete = err == nil
			if err != nil {
				level.Warn(logger).Log("msg", "stop visit marker heart beat due to error", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "blocks", blocksInfo, "err", err)
			}
			break heartBeat
		}
	}
	if isComplete {
		level.Info(logger).Log("msg", "update visit marker to completed status", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "blocks", blocksInfo)
		markBlocksVisitMarkerCompleted(context.Background(), bkt, logger, blocks, partitionedGroupID, partitionID, compactorID, blockVisitMarkerWriteFailed)
	}
	level.Info(logger).Log("msg", "stop visit marker heart beat", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "blocks", blocksInfo)
}

func markBlocksVisitMarkerCompleted(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	blocks []*metadata.Meta,
	partitionedGroupID uint32,
	partitionID int,
	compactorID string,
	blockVisitMarkerWriteFailed prometheus.Counter,
) {
	blockVisitMarker := BlockVisitMarker{
		VisitTime:          time.Now().Unix(),
		CompactorID:        compactorID,
		Status:             Completed,
		PartitionedGroupID: partitionedGroupID,
		PartitionID:        partitionID,
		Version:            VisitMarkerVersion1,
	}
	visitMarkerFileContent, err := json.Marshal(blockVisitMarker)
	if err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return
	}
	reader := bytes.NewReader(visitMarkerFileContent)
	for _, block := range blocks {
		blockID := block.ULID.String()
		if err := UpdateBlockVisitMarker(ctx, bkt, blockID, blockVisitMarker.PartitionID, reader, blockVisitMarkerWriteFailed); err != nil {
			level.Error(logger).Log("msg", "unable to upsert completed visit marker file content for block", "partitioned_group_id", blockVisitMarker.PartitionedGroupID, "partition_id", blockVisitMarker.PartitionID, "block_id", blockID, "err", err)
		} else {
			level.Info(logger).Log("msg", "block partition is completed", "partitioned_group_id", blockVisitMarker.PartitionedGroupID, "partition_id", blockVisitMarker.PartitionID, "block_id", blockID)
		}
		reader.Reset(visitMarkerFileContent)
	}
}

func IsBlockVisitMarker(path string) bool {
	return strings.HasSuffix(path, BlockVisitMarkerFileSuffix)
}

func IsNotBlockVisitMarkerError(err error) bool {
	return errors.Is(err, ErrorNotBlockVisitMarker)
}
