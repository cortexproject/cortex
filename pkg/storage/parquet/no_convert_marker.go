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
	NoConvertMarkerFileName = "parquet-no-convert-mark.json"

	CurrentNoConvertMarkVersion = NoConvertMarkVersion1
	NoConvertMarkVersion1       = 1

	NoConvertReasonTooManyLabels = "too_many_labels"
	NoConvertReasonMarkerExists  = "marker_exists"
)

type NoConvertMark struct {
	Version            int    `json:"version"`
	Reason             string `json:"reason"`
	LabelNamesCount    int    `json:"label_names_count,omitempty"`
	MaxBlockLabelNames int    `json:"max_block_label_names,omitempty"`
}

func ReadNoConvertMark(ctx context.Context, id ulid.ULID, userBkt objstore.InstrumentedBucket, logger log.Logger) (*NoConvertMark, error) {
	markerPath := path.Join(id.String(), NoConvertMarkerFileName)
	reader, err := userBkt.WithExpectedErrs(bucket.IsOneOfTheExpectedErrors(userBkt.IsAccessDeniedErr, userBkt.IsObjNotFoundErr)).Get(ctx, markerPath)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) || userBkt.IsAccessDeniedErr(err) {
			return &NoConvertMark{}, nil
		}

		return &NoConvertMark{}, err
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close parquet no-convert marker file reader")

	markerContent, err := io.ReadAll(reader)
	if err != nil {
		return &NoConvertMark{}, errors.Wrapf(err, "read file: %s", NoConvertMarkerFileName)
	}

	marker := NoConvertMark{}
	err = json.Unmarshal(markerContent, &marker)
	return &marker, err
}

func WriteNoConvertMark(ctx context.Context, id ulid.ULID, userBkt objstore.Bucket, labelNamesCount int, maxBlockLabelNames int) error {
	noConvertMarker := NoConvertMark{
		Version:            CurrentNoConvertMarkVersion,
		Reason:             NoConvertReasonTooManyLabels,
		LabelNamesCount:    labelNamesCount,
		MaxBlockLabelNames: maxBlockLabelNames,
	}
	noConvertMarkerPath := path.Join(id.String(), NoConvertMarkerFileName)
	b, err := json.Marshal(noConvertMarker)
	if err != nil {
		return err
	}
	return userBkt.Upload(ctx, noConvertMarkerPath, bytes.NewReader(b))
}

func ValidNoConvertMarkVersion(version int) bool {
	return version == NoConvertMarkVersion1
}

func (m NoConvertMark) ShouldSkipBlock(currentMaxBlockLabelNamesLimit int) bool {

	// limit=0 means the label-name guard is disabled,
	if currentMaxBlockLabelNamesLimit <= 0 {
		return false
	}

	// m.LabelNamesCount is recorded when the old no-convert marker was written
	return currentMaxBlockLabelNamesLimit < m.LabelNamesCount
}
