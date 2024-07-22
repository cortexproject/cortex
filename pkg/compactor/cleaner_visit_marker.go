package compactor

import (
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
	}
}

func (b *CleanerVisitMarker) IsExpired(cleanerVisitMarkerTimeout time.Duration) bool {
	return !time.Now().Before(time.Unix(b.VisitTime, 0).Add(cleanerVisitMarkerTimeout))
}

func (b *CleanerVisitMarker) IsVisited(cleanerVisitMarkerTimeout time.Duration) bool {
	return !b.IsCompleted() && !b.IsFailed() && !b.IsExpired(cleanerVisitMarkerTimeout)
}

func (b *CleanerVisitMarker) IsCompleted() bool {
	return b.Status == Completed
}

func (b *CleanerVisitMarker) IsFailed() bool {
	return b.Status == Failed
}

func (b *CleanerVisitMarker) IsInProgress() bool {
	return b.Status == InProgress
}

func (b *CleanerVisitMarker) IsPending() bool {
	return b.Status == Pending
}

func (b *CleanerVisitMarker) GetVisitMarkerFilePath() string {
	return GetCleanerVisitMarkerFilePath()
}

func (b *CleanerVisitMarker) MarkInProgress(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = InProgress
	b.VisitTime = time.Now().Unix()
}

func (b *CleanerVisitMarker) MarkPending(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = Pending
	b.VisitTime = time.Now().Unix()
}

func (b *CleanerVisitMarker) MarkCompleted(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = Completed
	b.VisitTime = time.Now().Unix()
}

func (b *CleanerVisitMarker) MarkFailed(ownerIdentifier string) {
	b.CompactorID = ownerIdentifier
	b.Status = Failed
	b.VisitTime = time.Now().Unix()
}

func (b *CleanerVisitMarker) LogInfo() []string {
	return []string{
		"compactor_id",
		b.CompactorID,
		"status",
		string(b.Status),
		"visit_time",
		time.Unix(b.VisitTime, 0).String(),
	}
}

func GetCleanerVisitMarkerFilePath() string {
	return path.Join(bucketindex.MarkersPathname, CleanerVisitMarkerName)
}
