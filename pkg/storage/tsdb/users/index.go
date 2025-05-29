package users

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/util/runutil"
)

const (
	userIndexVersion            = 1
	userIndexFilename           = "user-index.json"
	userIndexCompressedFilename = userIndexFilename + ".gz"
)

var (
	ErrIndexNotFound  = errors.New("user index not found")
	ErrIndexCorrupted = errors.New("user index corrupted")
)

type UserIndex struct {
	Version   int   `json:"version"`
	UpdatedAt int64 `json:"updated_at"`

	ActiveUsers   []string `json:"active"`
	DeletingUsers []string `json:"deleting"`
	DeletedUsers  []string `json:"deleted"`
}

func (idx *UserIndex) GetUpdatedAt() time.Time {
	return time.Unix(idx.UpdatedAt, 0)
}

// WriteUserIndex uploads the provided index to the storage.
func WriteUserIndex(ctx context.Context, bkt objstore.Bucket, idx *UserIndex) error {
	// Marshal the index.
	content, err := json.Marshal(idx)
	if err != nil {
		return errors.Wrap(err, "marshal user index")
	}

	// Compress it.
	var gzipContent bytes.Buffer
	gzip := gzip.NewWriter(&gzipContent)
	gzip.Name = userIndexFilename

	if _, err := gzip.Write(content); err != nil {
		return errors.Wrap(err, "gzip user index")
	}
	if err := gzip.Close(); err != nil {
		return errors.Wrap(err, "close gzip user index")
	}

	// Upload the index to the storage.
	if err := bkt.Upload(ctx, userIndexCompressedFilename, bytes.NewReader(gzipContent.Bytes())); err != nil {
		return errors.Wrap(err, "upload user index")
	}

	return nil
}

func ReadUserIndex(ctx context.Context, bkt objstore.InstrumentedBucket, logger log.Logger) (*UserIndex, error) {
	// Get the user index.
	reader, err := bkt.WithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, userIndexCompressedFilename)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, ErrIndexNotFound
		}

		return nil, errors.Wrap(err, "read user index")
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close user index reader")

	// Read all the content.
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, ErrIndexCorrupted
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close user index gzip reader")

	// Deserialize it.
	index := &UserIndex{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, ErrIndexCorrupted
	}

	return index, nil
}
