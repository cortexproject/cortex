package ingester

import "github.com/prometheus/common/model"

const (
	deletionStateNotDeleted = iota
	deletionStatePartiallyDeleted
	deletionStateFullyDeleted
)

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }

type serialDeletedIntervalsIterator struct {
	deletedIntervals          []model.Interval
	nextDeletedIntervalsIndex int
}

func newSerialDeletedIntervalsIterator(deletedIntervals []model.Interval) *serialDeletedIntervalsIterator {
	sitr := serialDeletedIntervalsIterator{deletedIntervals: deletedIntervals}
	if len(deletedIntervals) == 0 {
		sitr.deletedIntervals = nil
		sitr.nextDeletedIntervalsIndex = -1
	}

	return &sitr
}

func (s *serialDeletedIntervalsIterator) getOverlappingDeletedIntervals(from, through model.Time) (int, []model.Interval) {
	if s.nextDeletedIntervalsIndex == -1 || through < s.deletedIntervals[s.nextDeletedIntervalsIndex].Start {
		return deletionStateNotDeleted, nil
	}

	if s.deletedIntervals[s.nextDeletedIntervalsIndex].Start <= from && s.deletedIntervals[s.nextDeletedIntervalsIndex].End >= through {
		if s.deletedIntervals[s.nextDeletedIntervalsIndex].End == through {
			s.nextDeletedIntervalsIndex++
			if s.nextDeletedIntervalsIndex >= len(s.deletedIntervals) {
				s.nextDeletedIntervalsIndex = -1
			}
		}
		return deletionStateFullyDeleted, nil
	}

	overlappingDeletedIntervals := []model.Interval{}

	// checking whether batch overlaps next deleted interval. If yes, removing values which are overlapping
	for s.nextDeletedIntervalsIndex != -1 && through >= s.deletedIntervals[s.nextDeletedIntervalsIndex].Start {
		overlaps, overlappingIntervalStart, overlappingIntervalEnd := getOverlappingInterval(from, through, s.deletedIntervals[s.nextDeletedIntervalsIndex].Start, s.deletedIntervals[s.nextDeletedIntervalsIndex].End)
		if overlaps {
			overlappingDeletedIntervals = append(overlappingDeletedIntervals, model.Interval{Start: overlappingIntervalStart, End: overlappingIntervalEnd})
		}

		if through >= s.deletedIntervals[s.nextDeletedIntervalsIndex].End {
			s.nextDeletedIntervalsIndex++

			if s.nextDeletedIntervalsIndex >= len(s.deletedIntervals) {
				s.nextDeletedIntervalsIndex = -1
			}
		} else {
			break
		}
	}

	if len(overlappingDeletedIntervals) == 0 {
		return deletionStateNotDeleted, nil
	}

	return deletionStatePartiallyDeleted, overlappingDeletedIntervals
}

func getOverlappingInterval(from, to, fromInterval, toInterval model.Time) (bool, model.Time, model.Time) {
	if fromInterval > from {
		from = fromInterval
	}

	if toInterval < to {
		to = toInterval
	}
	return from < to, from, to
}
