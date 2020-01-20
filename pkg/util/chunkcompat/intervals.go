package chunkcompat

import (
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/common/model"
)

// ClientIntervalsToModelIntervals converts []*client.Interval to []model.Interval
func ClientIntervalsToModelIntervals(intervals []*client.Interval) []model.Interval {
	if len(intervals) == 0 {
		return nil
	}

	clientChunkIntervals := make([]model.Interval, len(intervals))

	for i := range intervals {
		clientChunkIntervals[i] = model.Interval{Start: model.Time(intervals[i].StartTimestampMs), End: model.Time(intervals[i].EndTimestampMs)}
	}

	return clientChunkIntervals
}

// ModelIntervalsToClientIntervals converts []model.Interval to []*client.Interval
func ModelIntervalsToClientIntervals(intervals []model.Interval) []*client.Interval {
	if len(intervals) == 0 {
		return nil
	}

	clientChunkIntervals := make([]*client.Interval, len(intervals))

	for i := range intervals {
		clientChunkIntervals[i] = &client.Interval{StartTimestampMs: int64(intervals[i].Start), EndTimestampMs: int64(intervals[i].End)}
	}

	return clientChunkIntervals
}
