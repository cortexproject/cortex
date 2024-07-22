package compactor

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
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
	ErrorNotPartitionVisitMarker = errors.New("file is not partition visit marker")
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

func NewPartitionVisitMarker(compactorID string, partitionedGroupID uint32, partitionID int) *PartitionVisitMarker {
	return &PartitionVisitMarker{
		CompactorID:        compactorID,
		PartitionedGroupID: partitionedGroupID,
		PartitionID:        partitionID,
	}
}

func (b *PartitionVisitMarker) IsExpired(partitionVisitMarkerTimeout time.Duration) bool {
	return !time.Now().Before(time.Unix(b.VisitTime, 0).Add(partitionVisitMarkerTimeout))
}

func (b *PartitionVisitMarker) IsVisited(partitionVisitMarkerTimeout time.Duration, partitionID int) bool {
	return b.IsCompleted() || (partitionID == b.PartitionID && !b.IsExpired(partitionVisitMarkerTimeout))
}

func (b *PartitionVisitMarker) IsPendingByCompactor(partitionVisitMarkerTimeout time.Duration, partitionID int, compactorID string) bool {
	return b.CompactorID == compactorID && partitionID == b.PartitionID && b.IsPending() && !b.IsExpired(partitionVisitMarkerTimeout)
}

func (b *PartitionVisitMarker) IsCompleted() bool {
	return b.Status == Completed
}

func (b *PartitionVisitMarker) IsFailed() bool {
	return b.Status == Failed
}

func (b *PartitionVisitMarker) IsPending() bool {
	return b.Status == Pending
}

func (b *PartitionVisitMarker) IsInProgress() bool {
	return b.Status == InProgress
}

func (b *PartitionVisitMarker) GetVisitMarkerFilePath() string {
	return GetPartitionVisitMarkerFilePath(b.PartitionedGroupID, b.PartitionID)
}

func (b *PartitionVisitMarker) MarkInProgress(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = InProgress
	b.VisitTime = time.Now().Unix()
}

func (b *PartitionVisitMarker) MarkPending(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = Pending
	b.VisitTime = time.Now().Unix()
}

func (b *PartitionVisitMarker) MarkCompleted(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = Completed
	b.VisitTime = time.Now().Unix()
}

func (b *PartitionVisitMarker) MarkFailed(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = Failed
	b.VisitTime = time.Now().Unix()
}

func (b *PartitionVisitMarker) LogInfo() []string {
	return []string{
		"visit_marker_partitioned_group_id",
		fmt.Sprintf("%d", b.PartitionedGroupID),
		"visit_marker_partition_id",
		fmt.Sprintf("%d", b.PartitionID),
		"visit_marker_compactor_id",
		b.CompactorID,
		"visit_marker_status",
		string(b.Status),
		"visit_marker_visit_time",
		time.Unix(b.VisitTime, 0).String(),
	}
}

func GetPartitionVisitMarkerFilePath(partitionedGroupID uint32, partitionID int) string {
	return path.Join(GetPartitionVisitMarkerDirectoryPath(partitionedGroupID), fmt.Sprintf("%s%d-%s", PartitionVisitMarkerFilePrefix, partitionID, PartitionVisitMarkerFileSuffix))
}

func GetPartitionVisitMarkerDirectoryPath(partitionedGroupID uint32) string {
	return path.Join(PartitionedGroupDirectory, PartitionVisitMarkerDirectory, fmt.Sprintf("%d", partitionedGroupID))
}

func IsPartitionVisitMarker(path string) bool {
	return strings.HasSuffix(path, PartitionVisitMarkerFileSuffix)
}

func IsNotPartitionVisitMarkerError(err error) bool {
	return errors.Is(err, ErrorNotPartitionVisitMarker)
}
