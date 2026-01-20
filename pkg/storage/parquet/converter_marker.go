package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

const (
	ConverterMarkerPrefix   = "parquet-markers"
	ConverterMarkerFileName = "parquet-converter-mark.json"

	CurrentVersion               = ParquetConverterMarkVersion2
	ParquetConverterMarkVersion1 = 1
	// ParquetConverterMarkVersion2 has an additional series hash
	// column which is used for projection pushdown.
	ParquetConverterMarkVersion2 = 2
)

type ConverterMark struct {
	Version int `json:"version"`
	// Shards is the number of parquet shards created for this block.
	// This field is optional for backward compatibility.
	Shards int `json:"shards,omitempty"`
}

func ReadConverterMark(ctx context.Context, id ulid.ULID, userBkt objstore.InstrumentedBucket, logger log.Logger) (*ConverterMark, error) {
	markerPath := path.Join(id.String(), ConverterMarkerFileName)
	reader, err := userBkt.WithExpectedErrs(bucket.IsOneOfTheExpectedErrors(userBkt.IsAccessDeniedErr, userBkt.IsObjNotFoundErr)).Get(ctx, markerPath)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) || userBkt.IsAccessDeniedErr(err) {
			return &ConverterMark{}, nil
		}

		return &ConverterMark{}, err
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close parquet converter marker file reader")

	metaContent, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read file: %s", ConverterMarkerFileName)
	}

	marker := ConverterMark{}
	err = json.Unmarshal(metaContent, &marker)
	return &marker, err
}

func WriteConverterMark(ctx context.Context, id ulid.ULID, userBkt objstore.Bucket, shards int) error {
	marker := ConverterMark{
		Version: CurrentVersion,
		Shards:  shards,
	}
	markerPath := path.Join(id.String(), ConverterMarkerFileName)
	b, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	return userBkt.Upload(ctx, markerPath, bytes.NewReader(b))
}

// ConverterMarkMeta is used in Bucket Index. It might not be the same as ConverterMark.
type ConverterMarkMeta struct {
	Version int `json:"version"`
	// Shards is the number of parquet shards created for this block.
	// This field is optional for backward compatibility.
	Shards int `json:"shards,omitempty"`
}

func ValidConverterMarkVersion(version int) bool {
	return version == ParquetConverterMarkVersion1 || version == ParquetConverterMarkVersion2
}
