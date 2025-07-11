package parquet

import (
	"github.com/oklog/ulid/v2"
)

type NoCompactMarkCheckFunc = func(bId ulid.ULID) bool

func ShouldConvertBlockToParquet(mint, maxt, noCompactMarkCheckAfter int64, timeRanges []int64, bId ulid.ULID, checkFunc NoCompactMarkCheckFunc) bool {
	// We assume timeRanges[0] is the TSDB block duration (2h), and we don't convert them.
	blockTimeRange := getBlockTimeRange(mint, maxt, timeRanges)
	if blockTimeRange > timeRanges[0] {
		return true
	}

	// We should check if 2h blocks have a `no-compact-mark.json` file
	// since these will never be compacted to 12h block.
	// We check if the `no-compact-mark.json` file exists only for blocks
	// after the noCompactMarkCheckAfter to reduce object storage calls.
	if mint >= noCompactMarkCheckAfter {
		if blockTimeRange == timeRanges[0] && checkFunc(bId) {
			return true
		}
	}
	return false
}

func getBlockTimeRange(mint, maxt int64, timeRanges []int64) int64 {
	timeRange := int64(0)
	// fallback logic to guess block time range based
	// on MaxTime and MinTime
	blockRange := maxt - mint
	for _, tr := range timeRanges {
		rangeStart := getRangeStart(mint, tr)
		rangeEnd := rangeStart + tr
		if tr >= blockRange && rangeEnd >= maxt {
			timeRange = tr
			break
		}
	}
	// If the block range is too big and cannot fit any configured time range, just fallback to the final time range.
	// This might not be accurate but should be good enough to decide if we want to convert the block to Parquet.
	// For this to work, at least 2 block ranges are required.
	if len(timeRanges) > 0 && timeRange == int64(0) {
		return timeRanges[len(timeRanges)-1]
	}
	return timeRange
}

func getRangeStart(mint, tr int64) int64 {
	// Compute start of aligned time range of size tr closest to the current block's start.
	// This code has been copied from TSDB.
	if mint >= 0 {
		return tr * (mint / tr)
	}

	return tr * ((mint - tr + 1) / tr)
}
