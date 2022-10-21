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
	// BlockVisitMarkerFile is the known json filename for representing the most recent compactor visit.
	BlockVisitMarkerFile = "visit-mark.json"
	// VisitMarkerVersion1 is the current supported version of visit-mark file.
	VisitMarkerVersion1 = 1
)

var (
	ErrorBlockVisitMarkerNotFound  = errors.New("block visit marker not found")
	ErrorUnmarshalBlockVisitMarker = errors.New("unmarshal block visit marker JSON")
)

type BlockVisitMarker struct {
	CompactorID string `json:"compactorID"`
	// VisitTime is a unix timestamp of when the block was visited (mark updated).
	VisitTime int64 `json:"visitTime"`
	// Version of the file.
	Version int `json:"version"`
}

func (b *BlockVisitMarker) isVisited(blockVisitMarkerTimeout time.Duration) bool {
	return time.Now().Before(time.Unix(b.VisitTime, 0).Add(blockVisitMarkerTimeout))
}

func (b *BlockVisitMarker) isVisitedByCompactor(blockVisitMarkerTimeout time.Duration, compactorID string) bool {
	return b.CompactorID == compactorID && time.Now().Before(time.Unix(b.VisitTime, 0).Add(blockVisitMarkerTimeout))
}

func ReadBlockVisitMarker(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, blockID string, blockVisitMarkerReadFailed prometheus.Counter) (*BlockVisitMarker, error) {
	visitMarkerFile := path.Join(blockID, BlockVisitMarkerFile)
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

func UpdateBlockVisitMarker(ctx context.Context, bkt objstore.Bucket, blockID string, reader io.Reader, blockVisitMarkerWriteFailed prometheus.Counter) error {
	blockVisitMarkerFilePath := path.Join(blockID, BlockVisitMarkerFile)
	if err := bkt.Upload(ctx, blockVisitMarkerFilePath, reader); err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return err
	}
	return nil
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
		blockID := block.ULID.String()
		if err := UpdateBlockVisitMarker(ctx, bkt, blockID, reader, blockVisitMarkerWriteFailed); err != nil {
			level.Error(logger).Log("msg", "unable to upsert visit marker file content for block", "blockID", blockID, "err", err)
		}
		reader.Reset(visitMarkerFileContent)
	}
}

func markBlocksVisitedHeartBeat(ctx context.Context, bkt objstore.Bucket, logger log.Logger, blocks []*metadata.Meta, compactorID string, blockVisitMarkerFileUpdateInterval time.Duration, blockVisitMarkerWriteFailed prometheus.Counter) {
	var blockIds []string
	for _, block := range blocks {
		blockIds = append(blockIds, block.ULID.String())
	}
	blocksInfo := strings.Join(blockIds, ",")
	level.Info(logger).Log("msg", fmt.Sprintf("start heart beat for blocks: %s", blocksInfo))
	ticker := time.NewTicker(blockVisitMarkerFileUpdateInterval)
	defer ticker.Stop()
heartBeat:
	for {
		level.Debug(logger).Log("msg", fmt.Sprintf("heart beat for blocks: %s", blocksInfo))
		blockVisitMarker := BlockVisitMarker{
			VisitTime:   time.Now().Unix(),
			CompactorID: compactorID,
			Version:     VisitMarkerVersion1,
		}
		markBlocksVisited(ctx, bkt, logger, blocks, blockVisitMarker, blockVisitMarkerWriteFailed)

		select {
		case <-ctx.Done():
			break heartBeat
		case <-ticker.C:
			continue
		}
	}
	level.Info(logger).Log("msg", fmt.Sprintf("stop heart beat for blocks: %s", blocksInfo))
}
