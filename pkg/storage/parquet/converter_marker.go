package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

const (
	ConverterMarkerPrefix   = "parquet-markers"
	ConverterMarkerFileName = "parquet-converter-mark.json"
	CurrentVersion          = 1
)

type ConverterMark struct {
	Version int `json:"version"`
}

func ReadConverterMark(ctx context.Context, id ulid.ULID, userBkt objstore.InstrumentedBucket, logger log.Logger) (*ConverterMark, error) {
	markerPath := path.Join(id.String(), ConverterMarkerFileName)
	reader, err := userBkt.WithExpectedErrs(tsdb.IsOneOfTheExpectedErrors(userBkt.IsAccessDeniedErr, userBkt.IsObjNotFoundErr)).Get(ctx, markerPath)
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

func WriteConverterMark(ctx context.Context, id ulid.ULID, userBkt objstore.Bucket) error {
	marker := ConverterMark{
		Version: CurrentVersion,
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
}
