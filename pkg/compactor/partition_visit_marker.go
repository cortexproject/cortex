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
	errorNotPartitionVisitMarker = errors.New("file is not partition visit marker")
)

type partitionVisitMarker struct {
	CompactorID        string      `json:"compactorID"`
	Status             VisitStatus `json:"status"`
	PartitionedGroupID uint32      `json:"partitionedGroupID"`
	PartitionID        int         `json:"partitionID"`
	// VisitTime is a unix timestamp of when the partition was visited (mark updated).
	VisitTime int64 `json:"visitTime"`
	// Version of the file.
	Version int `json:"version"`
}

func newPartitionVisitMarker(compactorID string, partitionedGroupID uint32, partitionID int) *partitionVisitMarker {
	return &partitionVisitMarker{
		CompactorID:        compactorID,
		PartitionedGroupID: partitionedGroupID,
		PartitionID:        partitionID,
	}
}

func (b *partitionVisitMarker) IsExpired(partitionVisitMarkerTimeout time.Duration) bool {
	return !time.Now().Before(time.Unix(b.VisitTime, 0).Add(partitionVisitMarkerTimeout))
}

func (b *partitionVisitMarker) IsVisited(partitionVisitMarkerTimeout time.Duration, partitionID int) bool {
	return b.GetStatus() == Completed || (partitionID == b.PartitionID && !b.IsExpired(partitionVisitMarkerTimeout))
}

func (b *partitionVisitMarker) IsPendingByCompactor(partitionVisitMarkerTimeout time.Duration, partitionID int, compactorID string) bool {
	return b.CompactorID == compactorID && partitionID == b.PartitionID && b.GetStatus() == Pending && !b.IsExpired(partitionVisitMarkerTimeout)
}

func (b *partitionVisitMarker) GetStatus() VisitStatus {
	return b.Status
}

func (b *partitionVisitMarker) GetVisitMarkerFilePath() string {
	return GetPartitionVisitMarkerFilePath(b.PartitionedGroupID, b.PartitionID)
}

func (b *partitionVisitMarker) UpdateStatus(ownerIdentifier string, status VisitStatus) {
	b.CompactorID = ownerIdentifier
	b.Status = status
	b.VisitTime = time.Now().Unix()
}

func (b *partitionVisitMarker) String() string {
	return fmt.Sprintf("visit_marker_partitioned_group_id=%d visit_marker_partition_id=%d visit_marker_compactor_id=%s visit_marker_status=%s visit_marker_visit_time=%s",
		b.PartitionedGroupID,
		b.PartitionID,
		b.CompactorID,
		b.Status,
		time.Unix(b.VisitTime, 0).String(),
	)
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
	return errors.Is(err, errorNotPartitionVisitMarker)
}
