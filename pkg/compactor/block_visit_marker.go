package compactor

import (
	"fmt"
	"path"
	"strings"

	"github.com/pkg/errors"
)

const (
	// BlockVisitMarkerFileSuffix is the known suffix of json filename for representing the most recent compactor visit.
	BlockVisitMarkerFileSuffix = "visit-mark.json"
	// BlockVisitMarkerFilePrefix is the known prefix of json filename for representing the most recent compactor visit.
	BlockVisitMarkerFilePrefix = "partition-"
	// VisitMarkerVersion1 is the current supported version of visit-mark file.
	VisitMarkerVersion1 = 1
)

var (
	ErrorNotBlockVisitMarker = errors.New("file is not block visit marker")
)

type BlockVisitMarker struct {
	CompactorID        string      `json:"compactorID"`
	Status             VisitStatus `json:"status"`
	PartitionedGroupID uint32      `json:"partitionedGroupID"`
	PartitionID        int         `json:"partitionID"`
	// VisitTime is a unix timestamp of when the block was visited (mark updated).
	VisitTime int64 `json:"visitTime"`
	// Version of the file.
	Version int `json:"version"`
}

func GetBlockVisitMarkerFile(blockID string, partitionID int) string {
	return path.Join(blockID, fmt.Sprintf("%s%d-%s", BlockVisitMarkerFilePrefix, partitionID, BlockVisitMarkerFileSuffix))
}

func IsBlockVisitMarker(path string) bool {
	return strings.HasSuffix(path, BlockVisitMarkerFileSuffix)
}

func IsNotBlockVisitMarkerError(err error) bool {
	return errors.Is(err, ErrorNotBlockVisitMarker)
}
