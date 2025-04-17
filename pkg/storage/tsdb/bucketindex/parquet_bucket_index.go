package bucketindex

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/go-kit/log"
	"github.com/klauspost/compress/gzip"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	parquetIndex               = "bucket-index-parquet.json"
	parquetIndexCompressedName = parquetIndex + ".gz"
)

type BlockWithExtension struct {
	*Block
	Extensions Extensions `json:"extensions,omitempty"`
}

type ParquetIndex struct {
	Blocks map[ulid.ULID]BlockWithExtension `json:"blocks"`
}

type Extensions struct {
	PartitionInfo PartitionInfo `json:"partition_info"`
}

type PartitionInfo struct {
	MetricNamePartitionCount int `json:"metric_name_partition_count"`
	MetricNamePartitionID    int `json:"metric_name_partition_id"`
}

func ReadParquetIndex(ctx context.Context, userBkt objstore.InstrumentedBucket, logger log.Logger) (*ParquetIndex, error) {
	// Get the bucket index.
	reader, err := userBkt.WithExpectedErrs(tsdb.IsOneOfTheExpectedErrors(userBkt.IsAccessDeniedErr, userBkt.IsObjNotFoundErr)).Get(ctx, parquetIndexCompressedName)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return &ParquetIndex{Blocks: map[ulid.ULID]BlockWithExtension{}}, nil
		}

		return nil, err
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close bucket index reader")

	// Read all the content.
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, ErrIndexCorrupted
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close bucket index gzip reader")

	// Deserialize it.
	index := &ParquetIndex{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, ErrIndexCorrupted
	}
	return index, nil
}

func WriteParquetIndex(ctx context.Context, bkt objstore.Bucket, idx *ParquetIndex) error {
	// Marshal the index.
	content, err := json.Marshal(idx)
	if err != nil {
		return errors.Wrap(err, "marshal bucket index")
	}

	// Compress it.
	var gzipContent bytes.Buffer
	gzip := gzip.NewWriter(&gzipContent)
	gzip.Name = parquetIndex

	if _, err := gzip.Write(content); err != nil {
		return errors.Wrap(err, "gzip bucket index")
	}
	if err := gzip.Close(); err != nil {
		return errors.Wrap(err, "close gzip bucket index")
	}

	// Upload the index to the storage.
	if err := bkt.Upload(ctx, parquetIndexCompressedName, bytes.NewReader(gzipContent.Bytes())); err != nil {
		return errors.Wrap(err, "upload bucket index")
	}

	return nil
}
