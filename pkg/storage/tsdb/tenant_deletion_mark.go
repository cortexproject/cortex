package tsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

const TenantDeletionMarkFile = "tenant-deletion-mark.json"

type TenantDeletionMark struct {
	// Unix timestamp when deletion marker was created.
	DeletionTime int64 `json:"deletion_time"`

	// Unix timestamp when cleanup was finished.
	FinishedTime int64 `json:"finished_time,omitempty"`
}

func NewTenantDeletionMark(deletionTime time.Time) *TenantDeletionMark {
	return &TenantDeletionMark{DeletionTime: deletionTime.Unix()}
}

// Checks for deletion mark for tenant. Errors other than "object not found" are returned.
func TenantDeletionMarkExists(ctx context.Context, bkt objstore.BucketReader, userID string) (bool, error) {
	markerFile := GetGlobalDeletionMarkPath(userID)
	if globalExists, err := exists(ctx, bkt, markerFile); err != nil {
		return false, err
	} else if globalExists {
		return true, nil
	}

	markerFile = GetLocalDeletionMarkPath(userID)
	return exists(ctx, bkt, markerFile)
}

// Uploads deletion mark to the tenant location in the bucket.
func WriteTenantDeletionMark(ctx context.Context, bkt objstore.Bucket, userID string, mark *TenantDeletionMark) error {
	markerFile := GetGlobalDeletionMarkPath(userID)
	return write(ctx, bkt, markerFile, mark)
}

// Returns tenant deletion mark for given user, if it exists. If it doesn't exist, returns nil mark, and no error.
func ReadTenantDeletionMark(ctx context.Context, bkt objstore.BucketReader, userID string) (*TenantDeletionMark, error) {
	markerFile := GetGlobalDeletionMarkPath(userID)
	if mark, err := read(ctx, bkt, markerFile); err != nil {
		return nil, err
	} else if mark != nil {
		return mark, nil
	}

	markerFile = GetLocalDeletionMarkPath(userID)
	return read(ctx, bkt, markerFile)
}

// Deletes the tenant deletion mark for given user if it exists.
func DeleteTenantDeletionMark(ctx context.Context, bkt objstore.Bucket, userID string) error {
	if err := bkt.Delete(ctx, GetGlobalDeletionMarkPath(userID)); err != nil {
		return err
	}
	if err := bkt.Delete(ctx, GetLocalDeletionMarkPath(userID)); err != nil {
		return err
	}
	return nil
}

func GetLocalDeletionMarkPath(userID string) string {
	return path.Join(userID, "markers", TenantDeletionMarkFile)
}

func GetGlobalDeletionMarkPath(userID string) string {
	return path.Join(util.GlobalMarkersDir, userID, TenantDeletionMarkFile)
}

func exists(ctx context.Context, bkt objstore.BucketReader, markerFile string) (bool, error) {
	return bkt.Exists(ctx, markerFile)
}

func write(ctx context.Context, bkt objstore.Bucket, markerFile string, mark *TenantDeletionMark) error {
	data, err := json.Marshal(mark)
	if err != nil {
		return errors.Wrap(err, "serialize tenant deletion mark")
	}

	return errors.Wrap(bkt.Upload(ctx, markerFile, bytes.NewReader(data)), "upload tenant deletion mark")
}

func read(ctx context.Context, bkt objstore.BucketReader, markerFile string) (*TenantDeletionMark, error) {
	r, err := bkt.Get(ctx, markerFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "failed to read deletion mark object: %s", markerFile)
	}

	mark := &TenantDeletionMark{}
	err = json.NewDecoder(r).Decode(mark)

	// Close reader before dealing with decode error.
	if closeErr := r.Close(); closeErr != nil {
		level.Warn(util_log.Logger).Log("msg", "failed to close bucket reader", "err", closeErr)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode deletion mark object: %s", markerFile)
	}

	return mark, nil
}
