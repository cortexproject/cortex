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
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

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

type PartitionedGroupStatus struct {
	PartitionedGroupID        uint32
	CanDelete                 bool
	IsCompleted               bool
	DeleteVisitMarker         bool
	PendingPartitions         int
	InProgressPartitions      int
	PendingOrFailedPartitions []Partition
}

func (s PartitionedGroupStatus) String() string {
	var partitions []string
	for _, p := range s.PendingOrFailedPartitions {
		partitions = append(partitions, fmt.Sprintf("%d", p.PartitionID))
	}
	return fmt.Sprintf(`{"partitioned_group_id": %d, "can_delete": %t, "is_complete": %t, "delete_visit_marker": %t, "pending_partitions": %d, "in_progress_partitions": %d, "pending_or_failed_partitions": [%s]}`,
		s.PartitionedGroupID, s.CanDelete, s.IsCompleted, s.DeleteVisitMarker, s.PendingPartitions, s.InProgressPartitions, strings.Join(partitions, ","))
}

type PartitionedGroupInfo struct {
	PartitionedGroupID uint32      `json:"partitionedGroupID"`
	PartitionCount     int         `json:"partitionCount"`
	Partitions         []Partition `json:"partitions"`
	RangeStart         int64       `json:"rangeStart"`
	RangeEnd           int64       `json:"rangeEnd"`
	CreationTime       int64       `json:"creationTime"`
	// Version of the file.
	Version int `json:"version"`
}

func (p *PartitionedGroupInfo) rangeStartTime() time.Time {
	return time.Unix(0, p.RangeStart*int64(time.Millisecond)).UTC()
}

func (p *PartitionedGroupInfo) rangeEndTime() time.Time {
	return time.Unix(0, p.RangeEnd*int64(time.Millisecond)).UTC()
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

func (p *PartitionedGroupInfo) getAllBlockIDs() []string {
	blocks := p.getAllBlocks()
	blockIDs := make([]string, len(blocks))
	for i, block := range blocks {
		blockIDs[i] = block.String()
	}
	return blockIDs
}

func (p *PartitionedGroupInfo) getPartitionedGroupStatus(
	ctx context.Context,
	userBucket objstore.InstrumentedBucket,
	partitionVisitMarkerTimeout time.Duration,
	userLogger log.Logger,
) PartitionedGroupStatus {
	status := PartitionedGroupStatus{
		PartitionedGroupID:        p.PartitionedGroupID,
		CanDelete:                 false,
		IsCompleted:               false,
		DeleteVisitMarker:         false,
		PendingPartitions:         0,
		InProgressPartitions:      0,
		PendingOrFailedPartitions: []Partition{},
	}
	allPartitionCompleted := true
	hasInProgressPartitions := false
	for _, partition := range p.Partitions {
		visitMarker := &partitionVisitMarker{
			PartitionedGroupID: p.PartitionedGroupID,
			PartitionID:        partition.PartitionID,
		}
		visitMarkerManager := NewVisitMarkerManager(userBucket, userLogger, "PartitionedGroupInfo.getPartitionedGroupStatus", visitMarker)
		partitionVisitMarkerExists := true
		if err := visitMarkerManager.ReadVisitMarker(ctx, visitMarker); err != nil {
			if errors.Is(err, errorVisitMarkerNotFound) {
				partitionVisitMarkerExists = false
			} else {
				level.Warn(userLogger).Log("msg", "unable to read partition visit marker", "path", visitMarker.GetVisitMarkerFilePath(), "err", err)
				return status
			}
		}

		if !partitionVisitMarkerExists {
			status.PendingPartitions++
			allPartitionCompleted = false
			status.PendingOrFailedPartitions = append(status.PendingOrFailedPartitions, partition)
		} else if visitMarker.VisitTime < p.CreationTime {
			status.DeleteVisitMarker = true
			allPartitionCompleted = false
		} else if (visitMarker.GetStatus() == Pending || visitMarker.GetStatus() == InProgress) && !visitMarker.IsExpired(partitionVisitMarkerTimeout) {
			status.InProgressPartitions++
			hasInProgressPartitions = true
			allPartitionCompleted = false
		} else if visitMarker.GetStatus() != Completed {
			status.PendingPartitions++
			allPartitionCompleted = false
			status.PendingOrFailedPartitions = append(status.PendingOrFailedPartitions, partition)
		}
	}

	if hasInProgressPartitions {
		return status
	}

	status.IsCompleted = allPartitionCompleted

	if allPartitionCompleted {
		status.CanDelete = true
		status.DeleteVisitMarker = true
		return status
	}

	checkedBlocks := make(map[ulid.ULID]struct{})
	for _, partition := range status.PendingOrFailedPartitions {
		for _, blockID := range partition.Blocks {
			if _, ok := checkedBlocks[blockID]; ok {
				continue
			}
			if !p.doesBlockExist(ctx, userBucket, userLogger, blockID) {
				level.Info(userLogger).Log("msg", "delete partitioned group", "reason", "block is physically deleted", "block", blockID)
				status.CanDelete = true
				status.DeleteVisitMarker = true
				return status
			}
			if p.isBlockDeleted(ctx, userBucket, userLogger, blockID) {
				level.Info(userLogger).Log("msg", "delete partitioned group", "reason", "block is marked for deletion", "block", blockID)
				status.CanDelete = true
				status.DeleteVisitMarker = true
				return status
			}
			if p.isBlockNoCompact(ctx, userBucket, userLogger, blockID) {
				level.Info(userLogger).Log("msg", "delete partitioned group", "reason", "block is marked for no compact", "block", blockID)
				status.CanDelete = true
				status.DeleteVisitMarker = true
				return status
			}
			checkedBlocks[blockID] = struct{}{}
		}
	}
	return status
}

func (p *PartitionedGroupInfo) doesBlockExist(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blockID ulid.ULID) bool {
	metaExists, err := userBucket.Exists(ctx, path.Join(blockID.String(), metadata.MetaFilename))
	if err != nil {
		level.Warn(userLogger).Log("msg", "unable to get stats of meta.json for block", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		return true
	}
	return metaExists
}

func (p *PartitionedGroupInfo) isBlockDeleted(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blockID ulid.ULID) bool {
	deletionMarkerExists, err := userBucket.Exists(ctx, path.Join(blockID.String(), metadata.DeletionMarkFilename))
	if err != nil {
		level.Warn(userLogger).Log("msg", "unable to get stats of deletion-mark.json for block", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		return false
	}
	return deletionMarkerExists
}

func (p *PartitionedGroupInfo) isBlockNoCompact(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blockID ulid.ULID) bool {
	noCompactMarkerExists, err := userBucket.Exists(ctx, path.Join(blockID.String(), metadata.NoCompactMarkFilename))
	if err != nil {
		level.Warn(userLogger).Log("msg", "unable to get stats of no-compact-mark.json for block", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		return false
	}
	return noCompactMarkerExists
}

func (p *PartitionedGroupInfo) markAllBlocksForDeletion(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blocksMarkedForDeletion *prometheus.CounterVec, userID string) error {
	blocks := p.getAllBlocks()
	deleteBlocksCount := 0
	defer func() {
		level.Info(userLogger).Log("msg", "total number of blocks marked for deletion during partitioned group info clean up", "count", deleteBlocksCount)
	}()
	for _, blockID := range blocks {
		if p.doesBlockExist(ctx, userBucket, userLogger, blockID) && !p.isBlockDeleted(ctx, userBucket, userLogger, blockID) {
			if err := block.MarkForDeletion(ctx, userLogger, userBucket, blockID, "delete block during partitioned group completion check", blocksMarkedForDeletion.WithLabelValues(userID, reasonValueRetention)); err != nil {
				level.Warn(userLogger).Log("msg", "unable to mark block for deletion", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
				return err
			}
			deleteBlocksCount++
			level.Debug(userLogger).Log("msg", "marked block for deletion during partitioned group info clean up", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		}
	}
	return nil
}

func (p *PartitionedGroupInfo) String() string {
	var partitions []string
	for _, partition := range p.Partitions {
		partitions = append(partitions, fmt.Sprintf("(PartitionID: %d, Blocks: %s)", partition.PartitionID, partition.Blocks))
	}
	return fmt.Sprintf("{PartitionedGroupID: %d, PartitionCount: %d, Partitions: %s}", p.PartitionedGroupID, p.PartitionCount, strings.Join(partitions, ", "))
}

func GetPartitionedGroupFile(partitionedGroupID uint32) string {
	return path.Join(PartitionedGroupDirectory, fmt.Sprintf("%d.json", partitionedGroupID))
}

func ReadPartitionedGroupInfo(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, partitionedGroupID uint32) (*PartitionedGroupInfo, error) {
	return ReadPartitionedGroupInfoFile(ctx, bkt, logger, GetPartitionedGroupFile(partitionedGroupID))
}

func ReadPartitionedGroupInfoFile(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, partitionedGroupFile string) (*PartitionedGroupInfo, error) {
	partitionedGroupReader, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, partitionedGroupFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, errors.Wrapf(ErrorPartitionedGroupInfoNotFound, "partitioned group file: %s", partitionedGroupReader)
		}
		return nil, errors.Wrapf(err, "get partitioned group file: %s", partitionedGroupReader)
	}
	defer runutil.CloseWithLogOnErr(logger, partitionedGroupReader, "close partitioned group reader")
	p, err := io.ReadAll(partitionedGroupReader)
	if err != nil {
		return nil, errors.Wrapf(err, "read partitioned group file: %s", partitionedGroupFile)
	}
	partitionedGroupInfo := PartitionedGroupInfo{}
	if err = json.Unmarshal(p, &partitionedGroupInfo); err != nil {
		return nil, errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: %v", partitionedGroupFile, err.Error())
	}
	if partitionedGroupInfo.Version != VisitMarkerVersion1 {
		return nil, errors.Errorf("unexpected partitioned group file version %d, expected %d", partitionedGroupInfo.Version, VisitMarkerVersion1)
	}
	if partitionedGroupInfo.CreationTime <= 0 {
		objAttr, err := bkt.Attributes(ctx, partitionedGroupFile)
		if err != nil {
			return nil, errors.Errorf("unable to get partitioned group file attributes: %s, error: %v", partitionedGroupFile, err.Error())
		}
		partitionedGroupInfo.CreationTime = objAttr.LastModified.Unix()
	}
	return &partitionedGroupInfo, nil
}

func UpdatePartitionedGroupInfo(ctx context.Context, bkt objstore.InstrumentedBucket, logger log.Logger, partitionedGroupInfo PartitionedGroupInfo) (*PartitionedGroupInfo, error) {
	// Ignore error in order to always update partitioned group info. There is no harm to put latest version of
	// partitioned group info which is supposed to be the correct grouping based on latest bucket store.
	existingPartitionedGroup, _ := ReadPartitionedGroupInfo(ctx, bkt, logger, partitionedGroupInfo.PartitionedGroupID)
	if existingPartitionedGroup != nil {
		level.Warn(logger).Log("msg", "partitioned group info already exists", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID)
		return existingPartitionedGroup, nil
	}
	if partitionedGroupInfo.CreationTime <= 0 {
		partitionedGroupInfo.CreationTime = time.Now().Unix()
	}
	partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupInfo.PartitionedGroupID)
	partitionedGroupInfoContent, err := json.Marshal(partitionedGroupInfo)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(partitionedGroupInfoContent)
	if err := bkt.Upload(ctx, partitionedGroupFile, reader); err != nil {
		return nil, err
	}
	level.Info(logger).Log("msg", "created new partitioned group info", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID)
	return &partitionedGroupInfo, nil
}
