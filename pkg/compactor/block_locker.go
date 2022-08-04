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
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

const BlockLockFile = "block.lock"

var (
	ErrorBlockLockNotFound  = errors.New("block lock not found")
	ErrorUnmarshalBlockLock = errors.New("unmarshal block lock JSON")
)

type BlockLocker struct {
	CompactorID string    `json:"compactorID"`
	LockTime    time.Time `json:"lockTime"`
}

func (b *BlockLocker) isLocked(blockLockTimeout time.Duration) bool {
	return time.Now().Before(b.LockTime.Add(blockLockTimeout))
}

func (b *BlockLocker) isLockedByCompactor(blockLockTimeout time.Duration, compactorID string) bool {
	return time.Now().Before(b.LockTime.Add(blockLockTimeout)) && b.CompactorID == compactorID
}

func ReadBlockLocker(ctx context.Context, bkt objstore.Bucket, blockID string, blockLockReadFailed prometheus.Counter) (*BlockLocker, error) {
	lockFile := path.Join(blockID, BlockLockFile)
	lockFileReader, err := bkt.Get(ctx, lockFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, errors.Wrapf(ErrorBlockLockNotFound, "block lock file: %s", lockFile)
		}
		blockLockReadFailed.Inc()
		return nil, errors.Wrapf(err, "get block lock file: %s", lockFile)
	}
	b, err := ioutil.ReadAll(lockFileReader)
	if err != nil {
		blockLockReadFailed.Inc()
		return nil, errors.Wrapf(err, "read block lock file: %s", lockFile)
	}
	blockLocker := BlockLocker{}
	err = json.Unmarshal(b, &blockLocker)
	if err != nil {
		blockLockReadFailed.Inc()
		return nil, errors.Wrapf(ErrorUnmarshalBlockLock, "block lock file: %s, error: %v", lockFile, err.Error())
	}
	return &blockLocker, nil
}

func UpdateBlockLocker(ctx context.Context, bkt objstore.Bucket, blockID string, compactorID string, blockLockWriteFailed prometheus.Counter) error {
	blockLockFilePath := path.Join(blockID, BlockLockFile)
	blockLocker := BlockLocker{
		CompactorID: compactorID,
		LockTime:    time.Now(),
	}
	lockFileContent, err := json.Marshal(blockLocker)
	if err != nil {
		blockLockWriteFailed.Inc()
		return err
	}
	err = bkt.Upload(ctx, blockLockFilePath, bytes.NewReader(lockFileContent))
	if err != nil {
		blockLockWriteFailed.Inc()
		return err
	}
	return nil
}

func LockBlocks(ctx context.Context, bkt objstore.Bucket, logger log.Logger, blocks []*metadata.Meta, compactorID string, blockLockWriteFailed prometheus.Counter) {
	for _, block := range blocks {
		blockID := block.ULID.String()
		err := UpdateBlockLocker(ctx, bkt, blockID, compactorID, blockLockWriteFailed)
		if err != nil {
			level.Error(logger).Log("msg", "unable to upsert lock file content for block", "blockID", blockID, "err", err)
		}
	}
}

func LockBlocksHeartBeat(ctx context.Context, bkt objstore.Bucket, logger log.Logger, blocks []*metadata.Meta, compactorID string, blockLockFileUpdateInterval time.Duration, blockLockWriteFailed prometheus.Counter) {
	var blockIds []string
	for _, block := range blocks {
		blockIds = append(blockIds, block.ULID.String())
	}
	blocksInfo := strings.Join(blockIds, ",")
	level.Info(logger).Log("msg", fmt.Sprintf("start heart beat for blocks: %s", blocksInfo))
heartBeat:
	for {
		select {
		case <-ctx.Done():
			break heartBeat
		default:
			level.Debug(logger).Log("msg", fmt.Sprintf("heart beat for blocks: %s", blocksInfo))
			LockBlocks(ctx, bkt, logger, blocks, compactorID, blockLockWriteFailed)
			time.Sleep(blockLockFileUpdateInterval)
		}
	}
	level.Info(logger).Log("msg", fmt.Sprintf("stop heart beat for blocks: %s", blocksInfo))
}
