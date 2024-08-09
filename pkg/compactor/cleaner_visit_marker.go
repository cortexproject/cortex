package compactor

import (
	"fmt"
	"path"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
)

const (
	// CleanerVisitMarkerName is the name of cleaner visit marker file.
	CleanerVisitMarkerName = "cleaner-visit-marker.json"
	// CleanerVisitMarkerVersion1 is the current supported version of cleaner visit mark file.
	CleanerVisitMarkerVersion1 = 1
)

type CleanerVisitMarker struct {
	CompactorID string      `json:"compactorID"`
	Status      VisitStatus `json:"status"`
	// VisitTime is a unix timestamp of when the partition was visited (mark updated).
	VisitTime int64 `json:"visitTime"`
	// Version of the file.
	Version int `json:"version"`
}

func NewCleanerVisitMarker(compactorID string) *CleanerVisitMarker {
	return &CleanerVisitMarker{
		CompactorID: compactorID,
		Version:     CleanerVisitMarkerVersion1,
	}
}

func (b *CleanerVisitMarker) IsExpired(cleanerVisitMarkerTimeout time.Duration) bool {
	return !time.Now().Before(time.Unix(b.VisitTime, 0).Add(cleanerVisitMarkerTimeout))
}

func (b *CleanerVisitMarker) IsVisited(cleanerVisitMarkerTimeout time.Duration) bool {
	return !(b.GetStatus() == Completed) && !(b.GetStatus() == Failed) && !b.IsExpired(cleanerVisitMarkerTimeout)
}

func (b *CleanerVisitMarker) GetStatus() VisitStatus {
	return b.Status
}

func (b *CleanerVisitMarker) GetVisitMarkerFilePath() string {
	return GetCleanerVisitMarkerFilePath()
}

func (b *CleanerVisitMarker) UpdateStatus(ownerIdentifier string, status VisitStatus) {
	b.CompactorID = ownerIdentifier
	b.Status = status
	b.VisitTime = time.Now().Unix()
}

func (b *CleanerVisitMarker) String() string {
	return fmt.Sprintf("compactor_id=%s status=%s visit_time=%s",
		b.CompactorID,
		b.Status,
		time.Unix(b.VisitTime, 0).String(),
	)
}

func GetCleanerVisitMarkerFilePath() string {
	return path.Join(bucketindex.MarkersPathname, CleanerVisitMarkerName)
}
