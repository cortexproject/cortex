package bucketindex

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

var (
	ErrIndexNotFound  = errors.New("bucket index not found")
	ErrIndexCorrupted = errors.New("bucket index corrupted")
)

// ReadIndex reads, parses and returns a bucket index from the bucket.
func ReadIndex(ctx context.Context, bkt objstore.Bucket, userID string, logger log.Logger) (*Index, error) {
	bkt = bucket.NewUserBucketClient(userID, bkt)

	// Get the bucket index.
	reader, err := bkt.Get(ctx, IndexCompressedFilename)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, ErrIndexNotFound
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

// WriteIndex generates the bucket index and writes it to the storage. If the old index is not
// passed in input, then the bucket index will be generated from scratch.
func WriteIndex(ctx context.Context, bkt objstore.Bucket, userID string, idx *Index) error {
	bkt = bucket.NewUserBucketClient(userID, bkt)

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

// DeleteIndex deletes the bucket index from the storage.
func DeleteIndex(ctx context.Context, bkt objstore.Bucket, userID string) error {
	bkt = bucket.NewUserBucketClient(userID, bkt)
	return errors.Wrap(bkt.Delete(ctx, IndexCompressedFilename), "delete bucket index")
}
