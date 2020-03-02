package intervals

import (
	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/ingester/client"
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

func GetOverlappingInterval(interval1, interval2 model.Interval) (bool, model.Interval) {
	if interval2.Start > interval1.Start {
		interval1.Start = interval2.Start
	}

	if interval2.End < interval1.End {
		interval1.End = interval2.End
	}

	return interval1.Start < interval1.End, interval1
}

// Intervals represent a set of continuous non-overlapping intervals
type Intervals []model.Interval

// GetOverlappingIntervalsFor returns overlapping intervals for given time range. It clamps the interval to include only overlaps
func (intervals Intervals) GetOverlappingIntervalsFor(interval model.Interval) Intervals {
	overlappingIntervals := Intervals{}

	for i := range intervals {
		overlaps, overlappingInterval := GetOverlappingInterval(intervals[i], interval)
		if !overlaps {
			continue
		}

		overlappingIntervals = append(overlappingIntervals, overlappingInterval)
	}

	return overlappingIntervals
}
