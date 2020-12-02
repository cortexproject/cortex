package bucketindex

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io/ioutil"

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

	content, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		return nil, errors.Wrap(err, "read bucket index")
	}

	// Deserialize it.
	index := &Index{}
	if err := json.Unmarshal(content, index); err != nil {
		return nil, ErrIndexCorrupted
	}

	return index, nil
}
