package parquet

func ShouldConvertBlockToParquet(mint, maxt int64, timeRanges []int64) bool {
	// We assume timeRanges[0] is the TSDB block duration (2h), and we don't convert them.
	return getBlockTimeRange(mint, maxt, timeRanges) > timeRanges[0]
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
