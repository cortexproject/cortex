package bucketindex

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_errors "github.com/cortexproject/cortex/pkg/util/errors"
	"github.com/cortexproject/cortex/pkg/util/runutil"
)

// SyncStatus is an enum for the possibles sync status.
type SyncStatus string

// Possible SyncStatus.
const (
	Ok                      SyncStatus = "Ok"
	GenericError            SyncStatus = "GenericError"
	CustomerManagedKeyError SyncStatus = "CustomerManagedKeyError"
	Unknown                 SyncStatus = "Unknown"
)

const (
	// SyncStatusFile is the known json filename for representing the most recent bucket index sync.
	SyncStatusFile = "bucket-index-sync-status.json"
	// SyncStatusFileVersion is the current supported version of bucket-index-sync-status.json file.
	SyncStatusFileVersion = 1
)

var (
	ErrIndexNotFound  = errors.New("bucket index not found")
	ErrIndexCorrupted = errors.New("bucket index corrupted")

	UnknownStatus = Status{
		Version:            SyncStatusFileVersion,
		Status:             Unknown,
		NonQueryableReason: Unknown,
	}
)

type Status struct {
	// SyncTime is a unix timestamp of when the bucket index was synced
	SyncTime int64 `json:"sync_ime"`
	// Version of the file.
	Version int `json:"version"`
	// Last Sync status
	Status SyncStatus `json:"status"`
	// Should not allow query until this time
	NonQueryableUntil int64 `json:"non_queryable_until"`
	// Should not allow query until this time
	NonQueryableReason SyncStatus `json:"non_queryable_reason"`
}

func (s *Status) GetNonQueryableUntil() time.Time {
	return time.Unix(s.NonQueryableUntil, 0)
}

// ReadIndex reads, parses and returns a bucket index from the bucket.
func ReadIndex(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, logger log.Logger) (*Index, error) {
	userBkt := bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	// Get the bucket index.
	reader, err := userBkt.WithExpectedErrs(tsdb.IsOneOfTheExpectedErrors(userBkt.IsAccessDeniedErr, userBkt.IsObjNotFoundErr)).Get(ctx, IndexCompressedFilename)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return nil, ErrIndexNotFound
		}

		if userBkt.IsAccessDeniedErr(err) {
			return nil, cortex_errors.WithCause(bucket.ErrCustomerManagedKeyAccessDenied, err)
		}

		return nil, errors.Wrap(err, "read bucket index")
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close bucket index reader")

	// Read all the content.
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, ErrIndexCorrupted
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close bucket index gzip reader")

	// Deserialize it.
	index := &Index{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, ErrIndexCorrupted
	}

	return index, nil
}

// WriteIndex uploads the provided index to the storage.
func WriteIndex(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, idx *Index) error {
	bkt = bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	// Marshal the index.
	content, err := json.Marshal(idx)
	if err != nil {
		return errors.Wrap(err, "marshal bucket index")
	}

	// Compress it.
	var gzipContent bytes.Buffer
	gzip := gzip.NewWriter(&gzipContent)
	gzip.Name = IndexFilename

	if _, err := gzip.Write(content); err != nil {
		return errors.Wrap(err, "gzip bucket index")
	}
	if err := gzip.Close(); err != nil {
		return errors.Wrap(err, "close gzip bucket index")
	}

	// Upload the index to the storage.
	if err := bkt.Upload(ctx, IndexCompressedFilename, &gzipContent); err != nil {
		return errors.Wrap(err, "upload bucket index")
	}

	return nil
}

// DeleteIndex deletes the bucket index from the storage. No error is returned if the index
// does not exist.
func DeleteIndex(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider) error {
	bkt = bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	err := bkt.Delete(ctx, IndexCompressedFilename)
	if err != nil && !bkt.IsObjNotFoundErr(err) {
		return errors.Wrap(err, "delete bucket index")
	}
	return nil
}

// DeleteIndexSyncStatus deletes the bucket index sync status file from the storage. No error is returned if the file
// does not exist.
func DeleteIndexSyncStatus(ctx context.Context, bkt objstore.Bucket, userID string) error {
	// Inject the user/tenant prefix.
	bkt = bucket.NewPrefixedBucketClient(bkt, userID)

	err := bkt.Delete(ctx, SyncStatusFile)
	if err != nil && !bkt.IsObjNotFoundErr(err) {
		return errors.Wrap(err, "delete bucket index")
	}
	return nil
}

// WriteSyncStatus upload the sync status file with the corresponding SyncStatus
// This file is not encrypted using the CMK configuration
func WriteSyncStatus(ctx context.Context, bkt objstore.Bucket, userID string, ss Status, logger log.Logger) {
	// Inject the user/tenant prefix.
	bkt = bucket.NewPrefixedBucketClient(bkt, userID)

	// Marshal the index.
	content, err := json.Marshal(ss)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to write bucket index status", "err", err)
		return
	}

	// Upload sync stats.
	if err := bkt.Upload(ctx, SyncStatusFile, bytes.NewReader(content)); err != nil {
		level.Warn(logger).Log("msg", "failed to upload index sync status", "err", err)
	}
}

// ReadSyncStatus retrieves the SyncStatus from the sync status file
// If the file is not found, it returns `Unknown`
func ReadSyncStatus(ctx context.Context, b objstore.Bucket, userID string, logger log.Logger) (Status, error) {
	// Inject the user/tenant prefix.
	bkt := bucket.NewPrefixedBucketClient(b, userID)

	reader, err := bkt.WithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, SyncStatusFile)

	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return UnknownStatus, nil
		}
		return UnknownStatus, err
	}

	defer runutil.CloseWithLogOnErr(logger, reader, "close sync status reader")

	content, err := io.ReadAll(reader)

	if err != nil {
		return UnknownStatus, err
	}

	s := Status{}
	if err = json.Unmarshal(content, &s); err != nil {
		return UnknownStatus, errors.Wrap(err, "error unmarshalling sync status")
	}
	if s.Version != SyncStatusFileVersion {
		return UnknownStatus, errors.New("bucket index sync version mismatch")
	}

	return s, nil
}
