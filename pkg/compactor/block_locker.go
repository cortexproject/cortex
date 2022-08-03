package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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

func (b *BlockLocker) isLockedForCompactor(blockLockTimeout time.Duration, compactorID string) bool {
	return time.Now().Before(b.LockTime.Add(blockLockTimeout)) && b.CompactorID != compactorID
}

func ReadBlockLocker(ctx context.Context, bkt objstore.Bucket, blockID string, blockLockReadFailed prometheus.Counter) (*BlockLocker, error) {
	lockFile := path.Join(blockID, BlockLockFile)
	lockFileReader, err := bkt.Get(ctx, lockFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, errors.Wrapf(ErrorBlockLockNotFound, "block lcok file: %s", lockFile)
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
		return nil, errors.Wrapf(ErrorUnmarshalBlockLock, "block lcok file: %s, error: %v", lockFile, err.Error())
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
