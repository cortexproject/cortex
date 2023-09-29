package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/util/runutil"
)

const (
	// PartitionVisitMarkerDirectory is the name of directory where all visit markers are saved.
	PartitionVisitMarkerDirectory = "visit-marks"
	// PartitionVisitMarkerFileSuffix is the known suffix of json filename for representing the most recent compactor visit.
	PartitionVisitMarkerFileSuffix = "visit-mark.json"
	// PartitionVisitMarkerFilePrefix is the known prefix of json filename for representing the most recent compactor visit.
	PartitionVisitMarkerFilePrefix = "partition-"
	// PartitionVisitMarkerVersion1 is the current supported version of visit-mark file.
	PartitionVisitMarkerVersion1 = 1
)

var (
	ErrorPartitionVisitMarkerNotFound  = errors.New("partition visit marker not found")
	ErrorUnmarshalPartitionVisitMarker = errors.New("unmarshal partition visit marker JSON")
	ErrorNotPartitionVisitMarker       = errors.New("file is not partition visit marker")
)

type VisitStatus string

const (
	Pending   VisitStatus = "pending"
	Completed VisitStatus = "completed"
)

type PartitionVisitMarker struct {
	CompactorID        string      `json:"compactorID"`
	Status             VisitStatus `json:"status"`
	PartitionedGroupID uint32      `json:"partitionedGroupID"`
	PartitionID        int         `json:"partitionID"`
	// VisitTime is a unix timestamp of when the partition was visited (mark updated).
	VisitTime int64 `json:"visitTime"`
	// Version of the file.
	Version int `json:"version"`
}

func (b *PartitionVisitMarker) isVisited(visitMarkerTimeout time.Duration, partitionID int) bool {
	return b.isCompleted() || partitionID == b.PartitionID && time.Now().Before(time.Unix(b.VisitTime, 0).Add(visitMarkerTimeout))
}

func (b *PartitionVisitMarker) isVisitedByCompactor(visitMarkerTimeout time.Duration, partitionID int, compactorID string) bool {
	return b.CompactorID == compactorID && b.isVisited(visitMarkerTimeout, partitionID)
}

func (b *PartitionVisitMarker) isCompleted() bool {
	return b.Status == Completed
}

func GetPartitionVisitMarkerFile(partitionedGroupID uint32, partitionID int) string {
	return path.Join(PartitionedGroupDirectory, PartitionVisitMarkerDirectory, fmt.Sprintf("%d", partitionedGroupID), fmt.Sprintf("%s%d-%s", PartitionVisitMarkerFilePrefix, partitionID, PartitionVisitMarkerFileSuffix))
}

func ReadPartitionVisitMarker(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, partitionedGroupID uint32, partitionID int, visitMarkerReadFailed prometheus.Counter) (*PartitionVisitMarker, error) {
	visitMarkerFile := GetPartitionVisitMarkerFile(partitionedGroupID, partitionID)
	visitMarkerFileReader, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, visitMarkerFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, errors.Wrapf(ErrorPartitionVisitMarkerNotFound, "partition visit marker file: %s", visitMarkerFile)
		}
		visitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(err, "get partition visit marker file: %s", visitMarkerFile)
	}
	defer runutil.CloseWithLogOnErr(logger, visitMarkerFileReader, "close partition visit marker reader")
	b, err := io.ReadAll(visitMarkerFileReader)
	if err != nil {
		visitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(err, "read partition visit marker file: %s", visitMarkerFile)
	}
	partitionVisitMarker := PartitionVisitMarker{}
	if err = json.Unmarshal(b, &partitionVisitMarker); err != nil {
		visitMarkerReadFailed.Inc()
		return nil, errors.Wrapf(ErrorUnmarshalPartitionVisitMarker, "partition visit marker file: %s, error: %v", visitMarkerFile, err.Error())
	}
	if partitionVisitMarker.Version != PartitionVisitMarkerVersion1 {
		return nil, errors.Errorf("unexpected partition visit mark file version %d, expected %d", partitionVisitMarker.Version, PartitionVisitMarkerVersion1)
	}
	return &partitionVisitMarker, nil
}

func UpdatePartitionVisitMarker(ctx context.Context, bkt objstore.Bucket, partitionedGroupID uint32, partitionID int, reader io.Reader, blockVisitMarkerWriteFailed prometheus.Counter) error {
	blockVisitMarkerFilePath := GetPartitionVisitMarkerFile(partitionedGroupID, partitionID)
	if err := bkt.Upload(ctx, blockVisitMarkerFilePath, reader); err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return err
	}
	return nil
}

func markPartitionVisited(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	marker PartitionVisitMarker,
	visitMarkerWriteFailed prometheus.Counter,
) {
	visitMarkerFileContent, err := json.Marshal(marker)
	if err != nil {
		visitMarkerWriteFailed.Inc()
		return
	}

	reader := bytes.NewReader(visitMarkerFileContent)
	if err := UpdatePartitionVisitMarker(ctx, bkt, marker.PartitionedGroupID, marker.PartitionID, reader, visitMarkerWriteFailed); err != nil {
		level.Error(logger).Log("msg", "unable to upsert visit marker file content for partition", "partitioned_group_id", marker.PartitionedGroupID, "partition_id", marker.PartitionID, "err", err)
	}
	level.Debug(logger).Log("msg", "marked partition visited", "partitioned_group_id", marker.PartitionedGroupID, "partition_id", marker.PartitionID)
}

func markPartitionVisitedHeartBeat(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	partitionedGroupID uint32,
	partitionID int,
	compactorID string,
	visitMarkerFileUpdateInterval time.Duration,
	visitMarkerWriteFailed prometheus.Counter,
	errChan chan error,
) {
	level.Info(logger).Log("msg", "start partition visit marker heart beat", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID)
	ticker := time.NewTicker(visitMarkerFileUpdateInterval)
	defer ticker.Stop()
	isComplete := false
heartBeat:
	for {
		blockVisitMarker := PartitionVisitMarker{
			VisitTime:          time.Now().Unix(),
			CompactorID:        compactorID,
			Status:             Pending,
			PartitionedGroupID: partitionedGroupID,
			PartitionID:        partitionID,
			Version:            PartitionVisitMarkerVersion1,
		}
		markPartitionVisited(ctx, bkt, logger, blockVisitMarker, visitMarkerWriteFailed)

		select {
		case <-ctx.Done():
			level.Warn(logger).Log("msg", "partition visit marker heart beat got cancelled", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID)
			break heartBeat
		case <-ticker.C:
			continue
		case err := <-errChan:
			isComplete = err == nil
			if err != nil {
				level.Warn(logger).Log("msg", "stop partition visit marker heart beat due to error", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "err", err)
			}
			break heartBeat
		}
	}
	if isComplete {
		level.Info(logger).Log("msg", "update partition visit marker to completed status", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID)
		markPartitionVisitMarkerCompleted(context.Background(), bkt, logger, partitionedGroupID, partitionID, compactorID, visitMarkerWriteFailed)
	}
	level.Info(logger).Log("msg", "stop partition visit marker heart beat", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID)
}

func markPartitionVisitMarkerCompleted(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	partitionedGroupID uint32,
	partitionID int,
	compactorID string,
	blockVisitMarkerWriteFailed prometheus.Counter,
) {
	visitMarker := PartitionVisitMarker{
		VisitTime:          time.Now().Unix(),
		CompactorID:        compactorID,
		Status:             Completed,
		PartitionedGroupID: partitionedGroupID,
		PartitionID:        partitionID,
		Version:            PartitionVisitMarkerVersion1,
	}
	visitMarkerFileContent, err := json.Marshal(visitMarker)
	if err != nil {
		blockVisitMarkerWriteFailed.Inc()
		return
	}
	reader := bytes.NewReader(visitMarkerFileContent)
	if err := UpdatePartitionVisitMarker(ctx, bkt, visitMarker.PartitionedGroupID, visitMarker.PartitionID, reader, blockVisitMarkerWriteFailed); err != nil {
		level.Error(logger).Log("msg", "unable to upsert completed visit marker file content for partition", "partitioned_group_id", visitMarker.PartitionedGroupID, "partition_id", visitMarker.PartitionID, "err", err)
	} else {
		level.Info(logger).Log("msg", "partition is completed", "partitioned_group_id", visitMarker.PartitionedGroupID, "partition_id", visitMarker.PartitionID)
	}
}

func IsPartitionVisitMarker(path string) bool {
	return strings.HasSuffix(path, PartitionVisitMarkerFileSuffix)
}

func IsNotPartitionVisitMarkerError(err error) bool {
	return errors.Is(err, ErrorNotPartitionVisitMarker)
}
