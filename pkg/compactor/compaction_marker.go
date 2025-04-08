package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	ParquetCompactionMakerFileName = "parquet-compaction-mark.json"
	CurrentVersion                 = 6
)

type CompactionMark struct {
	Version int `json:"version"`
}

func (m *CompactionMark) markerFilename() string { return ParquetCompactionMakerFileName }

func ReadCompactMark(ctx context.Context, id ulid.ULID, userBkt objstore.InstrumentedBucket, logger log.Logger) (*CompactionMark, error) {
	markerPath := path.Join(id.String(), ParquetCompactionMakerFileName)
	reader, err := userBkt.WithExpectedErrs(tsdb.IsOneOfTheExpectedErrors(userBkt.IsAccessDeniedErr, userBkt.IsObjNotFoundErr)).Get(ctx, markerPath)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return &CompactionMark{}, nil
		}

		return &CompactionMark{}, err
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close bucket index reader")

	metaContent, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read file: %s", ParquetCompactionMakerFileName)
	}

	marker := CompactionMark{}
	err = json.Unmarshal(metaContent, &marker)
	return &marker, err
}

func WriteCompactMark(ctx context.Context, id ulid.ULID, userBkt objstore.Bucket) error {
	marker := CompactionMark{
		Version: CurrentVersion,
	}
	markerPath := path.Join(id.String(), ParquetCompactionMakerFileName)
	b, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	return userBkt.Upload(ctx, markerPath, bytes.NewReader(b))
}
