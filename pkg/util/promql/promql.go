package promql

import (
	"math"
	"sort"
	"time"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/cortexproject/cortex/pkg/util"
)

// FindNonOverlapQueryLength iterates through all the vector selectors in the statement and finds the time interval
// each selector will try to process. It merges intervals to be non overlapping and calculates the total duration as
// the query length. This takes into account offsets, @ modifiers, and range selectors.
// If the statement does not select series, then duration 0 will be returned.
func FindNonOverlapQueryLength(expr parser.Expr, start, end int64, lookbackDelta time.Duration) time.Duration {
	type minMaxTime struct {
		minTime, maxTime int64
	}
	intervals := make([]minMaxTime, 0)

	// Whenever a MatrixSelector is evaluated, evalRange is set to the corresponding range.
	// The evaluation of the VectorSelector inside then evaluates the given range and unsets
	// the variable.
	var evalRange time.Duration
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			start, end := getTimeRangesForSelector(start, end, durationMilliseconds(lookbackDelta), n, path, evalRange)
			intervals = append(intervals, minMaxTime{start, end})
			evalRange = 0
		case *parser.MatrixSelector:
			evalRange = n.Range
		}
		return nil
	})

	if len(intervals) == 0 {
		return 0
	}

	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].minTime < intervals[j].minTime
	})

	prev := intervals[0]
	length := time.Duration(0)
	for i := 1; i < len(intervals); i++ {
		if intervals[i].minTime <= prev.maxTime {
			prev.maxTime = max(prev.maxTime, intervals[i].maxTime)
		} else {
			length += util.TimeFromMillis(prev.maxTime).Sub(util.TimeFromMillis(prev.minTime))
			prev = intervals[i]
		}
	}
	length += util.TimeFromMillis(prev.maxTime).Sub(util.TimeFromMillis(prev.minTime))
	return length
}

// Copied from https://github.com/prometheus/prometheus/blob/v2.52.0/promql/engine.go#L863.
func getTimeRangesForSelector(start, end, lookbackDelta int64, n *parser.VectorSelector, path []parser.Node, evalRange time.Duration) (int64, int64) {
	subqOffset, subqRange, subqTs := subqueryTimes(path)

	if subqTs != nil {
		// The timestamp on the subquery overrides the eval statement time ranges.
		start = *subqTs
		end = *subqTs
	}

	if n.Timestamp != nil {
		// The timestamp on the selector overrides everything.
		start = *n.Timestamp
		end = *n.Timestamp
	} else {
		offsetMilliseconds := durationMilliseconds(subqOffset)
		start = start - offsetMilliseconds - durationMilliseconds(subqRange)
		end -= offsetMilliseconds
	}

	if evalRange == 0 {
		start -= lookbackDelta
	} else {
		// For all matrix queries we want to ensure that we have (end-start) + range selected
		// this way we have `range` data before the start time
		start -= durationMilliseconds(evalRange)
	}

	offsetMilliseconds := durationMilliseconds(n.OriginalOffset)
	start -= offsetMilliseconds
	end -= offsetMilliseconds

	return start, end
}

// subqueryTimes returns the sum of offsets and ranges of all subqueries in the path.
// If the @ modifier is used, then the offset and range is w.r.t. that timestamp
// (i.e. the sum is reset when we have @ modifier).
// The returned *int64 is the closest timestamp that was seen. nil for no @ modifier.
// Copied from https://github.com/prometheus/prometheus/blob/v2.52.0/promql/engine.go#L803.
func subqueryTimes(path []parser.Node) (time.Duration, time.Duration, *int64) {
	var (
		subqOffset, subqRange time.Duration
		ts                    int64 = math.MaxInt64
	)
	for _, node := range path {
		if n, ok := node.(*parser.SubqueryExpr); ok {
			subqOffset += n.OriginalOffset
			subqRange += n.Range
			if n.Timestamp != nil {
				// The @ modifier on subquery invalidates all the offset and
				// range till now. Hence resetting it here.
				subqOffset = n.OriginalOffset
				subqRange = n.Range
				ts = *n.Timestamp
			}
		}
	}
	var tsp *int64
	if ts != math.MaxInt64 {
		tsp = &ts
	}
	return subqOffset, subqRange, tsp
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}
