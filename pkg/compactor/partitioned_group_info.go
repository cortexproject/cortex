package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/util/runutil"
)

const (
	PartitionedGroupDirectory    = "partitioned-groups"
	PartitionedGroupInfoVersion1 = 1
)

var (
	ErrorPartitionedGroupInfoNotFound  = errors.New("partitioned group info not found")
	ErrorUnmarshalPartitionedGroupInfo = errors.New("unmarshal partitioned group info JSON")
)

type Partition struct {
	PartitionID int         `json:"partitionID"`
	Blocks      []ulid.ULID `json:"blocks"`
}

func (p *Partition) getBlocksSet() map[ulid.ULID]struct{} {
	res := make(map[ulid.ULID]struct{})
	for _, blockID := range p.Blocks {
		res[blockID] = struct{}{}
	}
	return res
}

type PartitionedGroupInfo struct {
	PartitionedGroupID uint32      `json:"partitionedGroupID"`
	PartitionCount     int         `json:"partitionCount"`
	Partitions         []Partition `json:"partitions"`
	RangeStart         int64       `json:"rangeStart"`
	RangeEnd           int64       `json:"rangeEnd"`
	// Version of the file.
	Version int `json:"version"`
}

func (p *PartitionedGroupInfo) getPartitionIDsByBlock(blockID ulid.ULID) []int {
	var partitionIDs []int
partitionLoop:
	for _, partition := range p.Partitions {
		for _, block := range partition.Blocks {
			if block == blockID {
				partitionIDs = append(partitionIDs, partition.PartitionID)
				continue partitionLoop
			}
		}
	}
	return partitionIDs
}

func (p *PartitionedGroupInfo) getAllBlocks() []ulid.ULID {
	uniqueBlocks := make(map[ulid.ULID]struct{})
	for _, partition := range p.Partitions {
		for _, block := range partition.Blocks {
			uniqueBlocks[block] = struct{}{}
		}
	}
	blocks := make([]ulid.ULID, len(uniqueBlocks))
	i := 0
	for block := range uniqueBlocks {
		blocks[i] = block
		i++
	}
	return blocks
}

func (p PartitionedGroupInfo) String() string {
	var partitions []string
	for _, partition := range p.Partitions {
		partitions = append(partitions, fmt.Sprintf("(PartitionID: %d, Blocks: %s)", partition.PartitionID, partition.Blocks))
	}
	return fmt.Sprintf("{PartitionedGroupID: %d, PartitionCount: %d, Partitions: %s}", p.PartitionedGroupID, p.PartitionCount, strings.Join(partitions, ", "))
}

func GetPartitionedGroupFile(partitionedGroupID uint32) string {
	return path.Join(PartitionedGroupDirectory, fmt.Sprintf("%d.json", partitionedGroupID))
}

func ReadPartitionedGroupInfo(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, partitionedGroupID uint32, partitionedGroupInfoReadFailed prometheus.Counter) (*PartitionedGroupInfo, error) {
	return ReadPartitionedGroupInfoFile(ctx, bkt, logger, GetPartitionedGroupFile(partitionedGroupID), partitionedGroupInfoReadFailed)
}

func ReadPartitionedGroupInfoFile(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, partitionedGroupFile string, partitionedGroupInfoReadFailed prometheus.Counter) (*PartitionedGroupInfo, error) {
	partitionedGroupReader, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, partitionedGroupFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, errors.Wrapf(ErrorPartitionedGroupInfoNotFound, "partitioned group file: %s", partitionedGroupReader)
		}
		partitionedGroupInfoReadFailed.Inc()
		return nil, errors.Wrapf(err, "get partitioned group file: %s", partitionedGroupReader)
	}
	defer runutil.CloseWithLogOnErr(logger, partitionedGroupReader, "close partitioned group reader")
	p, err := io.ReadAll(partitionedGroupReader)
	if err != nil {
		partitionedGroupInfoReadFailed.Inc()
		return nil, errors.Wrapf(err, "read partitioned group file: %s", partitionedGroupFile)
	}
	partitionedGroupInfo := PartitionedGroupInfo{}
	if err = json.Unmarshal(p, &partitionedGroupInfo); err != nil {
		partitionedGroupInfoReadFailed.Inc()
		return nil, errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: %v", partitionedGroupFile, err.Error())
	}
	if partitionedGroupInfo.Version != VisitMarkerVersion1 {
		partitionedGroupInfoReadFailed.Inc()
		return nil, errors.Errorf("unexpected partitioned group file version %d, expected %d", partitionedGroupInfo.Version, VisitMarkerVersion1)
	}
	return &partitionedGroupInfo, nil
}

func UpdatePartitionedGroupInfo(ctx context.Context, bkt objstore.InstrumentedBucket, logger log.Logger, partitionedGroupInfo PartitionedGroupInfo, partitionedGroupInfoReadFailed prometheus.Counter, partitionedGroupInfoWriteFailed prometheus.Counter) (*PartitionedGroupInfo, error) {
	existingPartitionedGroup, _ := ReadPartitionedGroupInfo(ctx, bkt, logger, partitionedGroupInfo.PartitionedGroupID, partitionedGroupInfoReadFailed)
	if existingPartitionedGroup != nil {
		level.Warn(logger).Log("msg", "partitioned group info already exists", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID)
		return existingPartitionedGroup, nil
	}
	partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupInfo.PartitionedGroupID)
	partitionedGroupInfoContent, err := json.Marshal(partitionedGroupInfo)
	if err != nil {
		partitionedGroupInfoWriteFailed.Inc()
		return nil, err
	}
	reader := bytes.NewReader(partitionedGroupInfoContent)
	if err := bkt.Upload(ctx, partitionedGroupFile, reader); err != nil {
		return nil, err
	}
	return &partitionedGroupInfo, nil
}
