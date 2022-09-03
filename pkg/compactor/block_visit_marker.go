package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const BlockVisitMarkerFile = "block.visit"

var (
	ErrorBlockVisitMarkerNotFound  = errors.New("block visit marker not found")
	ErrorUnmarshalBlockVisitMarker = errors.New("unmarshal block visit marker JSON")
)

type BlockVisitMarker struct {
	CompactorID string    `json:"compactorID"`
	VisitTime   time.Time `json:"visitTime"`
}

func (b *BlockVisitMarker) isVisited(blockVisitMarkerTimeout time.Duration) bool {
	return time.Now().Before(b.VisitTime.Add(blockVisitMarkerTimeout))
}

func (b *BlockVisitMarker) isVisitedByCompactor(blockVisitMarkerTimeout time.Duration, compactorID string) bool {
	return time.Now().Before(b.VisitTime.Add(blockVisitMarkerTimeout)) && b.CompactorID == compactorID
}

func ReadBlockVisitMarker(ctx context.Context, bkt objstore.Bucket, blockID string, blockVisitMarkerReadFailed prometheus.Counter) (*BlockVisitMarker, error) {
	visitMarkerFile := path.Join(blockID, BlockVisitMarkerFile)
	visitMarkerFileReader, err := bkt.Get(ctx, visitMarkerFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, errors.Wrapf(ErrorBlockVisitMarkerNotFound, "block visit marker file: %s", visitMarkerFile)
		}
		blockVisitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(err, "get block visit marker file: %s", visitMarkerFile)
	}
	b, err := ioutil.ReadAll(visitMarkerFileReader)
	if err != nil {
		blockVisitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(err, "read block visit marker file: %s", visitMarkerFile)
	}
	blockVisitMarker := BlockVisitMarker{}
	err = json.Unmarshal(b, &blockVisitMarker)
	if err != nil {
		blockVisitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(ErrorUnmarshalBlockVisitMarker, "block visit marker file: %s, error: %v", visitMarkerFile, err.Error())
	}
	return &blockVisitMarker, nil
}

func UpdateBlockVisitMarker(ctx context.Context, bkt objstore.Bucket, blockID string, compactorID string, blockVisitMarkerWriteFailed prometheus.Counter) error {
	blockVisitMarkerFilePath := path.Join(blockID, BlockVisitMarkerFile)
	blockVisitMarker := BlockVisitMarker{
		CompactorID: compactorID,
		VisitTime:   time.Now(),
	}
	visitMarkerFileContent, err := json.Marshal(blockVisitMarker)
	if err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return err
	}
	err = bkt.Upload(ctx, blockVisitMarkerFilePath, bytes.NewReader(visitMarkerFileContent))
	if err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return err
	}
	return nil
}

func markBlocksVisited(ctx context.Context, bkt objstore.Bucket, logger log.Logger, blocks []*metadata.Meta, compactorID string, blockVisitMarkerWriteFailed prometheus.Counter) {
	for _, block := range blocks {
		blockID := block.ULID.String()
		err := UpdateBlockVisitMarker(ctx, bkt, blockID, compactorID, blockVisitMarkerWriteFailed)
		if err != nil {
			level.Error(logger).Log("msg", "unable to upsert visit marker file content for block", "blockID", blockID, "err", err)
		}
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
		markBlocksVisited(ctx, bkt, logger, blocks, compactorID, blockVisitMarkerWriteFailed)

		select {
		case <-ctx.Done():
			break heartBeat
		case <-ticker.C:
			continue
		}
	}
	level.Info(logger).Log("msg", fmt.Sprintf("stop heart beat for blocks: %s", blocksInfo))
}
