package util

import (
	"context"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"
)

const (
	nanosecondsInMillisecond = int64(time.Millisecond / time.Nanosecond)
)

func TimeToMillis(t time.Time) int64 {
	return t.UnixNano() / nanosecondsInMillisecond
}

// TimeFromMillis is a helper to turn milliseconds -> time.Time
func TimeFromMillis(ms int64) time.Time {
	return time.Unix(0, ms*nanosecondsInMillisecond)
}

// FormatTimeMillis returns a human readable version of the input time (in milliseconds).
func FormatTimeMillis(ms int64) string {
	return TimeFromMillis(ms).String()
}

// FormatTimeModel returns a human readable version of the input time.
func FormatTimeModel(t model.Time) string {
	return TimeFromMillis(int64(t)).String()
}

func FormatMillisToSeconds(ms int64) string {
	return strconv.FormatFloat(float64(ms)/float64(1000), 'f', -1, 64)
}

// ParseTime parses the string into an int64, milliseconds since epoch.
func ParseTime(s string) (int64, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		tm := time.Unix(int64(s), int64(ns*float64(time.Second)))
		return TimeToMillis(tm), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return TimeToMillis(t), nil
	}
	return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid timestamp", s)
}

// ParseTimeParam parses the time request parameter into an int64, milliseconds since epoch.
func ParseTimeParam(r *http.Request, paramName string, defaultValue int64) (int64, error) {
	val := r.FormValue(paramName)
	if val == "" {
		val = strconv.FormatInt(defaultValue, 10)
	}
	result, err := ParseTime(val)
	if err != nil {
		return 0, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}

// DurationWithJitter returns random duration from "input - input*variance" to "input + input*variance" interval.
func DurationWithJitter(input time.Duration, variancePerc float64) time.Duration {
	// No duration? No jitter.
	if input == 0 {
		return 0
	}

	variance := int64(float64(input) * variancePerc)
	jitter := rand.Int63n(variance*2) - variance

	return input + time.Duration(jitter)
}

// DurationWithPositiveJitter returns random duration from "input" to "input + input*variance" interval.
func DurationWithPositiveJitter(input time.Duration, variancePerc float64) time.Duration {
	// No duration? No jitter.
	if input == 0 {
		return 0
	}

	variance := int64(float64(input) * variancePerc)
	jitter := rand.Int63n(variance)

	return input + time.Duration(jitter)
}

// PositiveJitter returns random duration from "0" to "input*variance" interval.
func PositiveJitter(input time.Duration, variancePerc float64) time.Duration {
	// No duration or no variancePerc? No jitter.
	if input == 0 || variancePerc == 0 {
		return 0
	}

	variance := int64(float64(input) * variancePerc)
	jitter := rand.Int63n(variance)

	return time.Duration(jitter)
}

// NewDisableableTicker essentially wraps NewTicker but allows the ticker to be disabled by passing
// zero duration as the interval. Returns a function for stopping the ticker, and the ticker channel.
func NewDisableableTicker(interval time.Duration) (func(), <-chan time.Time) {
	if interval == 0 {
		return func() {}, nil
	}

	tick := time.NewTicker(interval)
	return func() { tick.Stop() }, tick.C
}

// FindMinMaxTime returns the time in milliseconds of the earliest and latest point in time the statement will try to process.
// This takes into account offsets, @ modifiers, and range selectors.
// If the expression does not select series, then FindMinMaxTime returns (0, 0).
func FindMinMaxTime(r *http.Request, expr parser.Expr, lookbackDelta time.Duration, now time.Time) (int64, int64) {
	isQuery := strings.HasSuffix(r.URL.Path, "/query")

	var startTime, endTime int64
	if isQuery {
		if t, err := ParseTimeParam(r, "time", now.UnixMilli()); err == nil {
			startTime = t
			endTime = t
		}
	} else {
		if st, err := ParseTime(r.FormValue("start")); err == nil {
			if et, err := ParseTime(r.FormValue("end")); err == nil {
				startTime = st
				endTime = et
			}
		}
	}

	es := &parser.EvalStmt{
		Expr:          expr,
		Start:         TimeFromMillis(startTime),
		End:           TimeFromMillis(endTime),
		LookbackDelta: lookbackDelta,
	}

	return promql.FindMinMaxTime(es)
}

// SlotInfoFunc returns the slot number and the total number of slots
type SlotInfoFunc func() (int, int)

type SlottedTicker struct {
	C <-chan time.Time // The channel on which the ticks are delivered.

	done        func()
	d           time.Duration
	shouldReset bool
	ticker      *time.Ticker
	sf          SlotInfoFunc
	slotJitter  float64
}

func NewSlottedTicker(sf SlotInfoFunc, d time.Duration, slotJitter float64) *SlottedTicker {
	c := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	st := &SlottedTicker{
		C:           c,
		done:        cancel,
		d:           d,
		sf:          sf,
		shouldReset: true,
		slotJitter:  slotJitter,
	}
	slitIndex, totalSlots := sf()
	st.ticker = time.NewTicker(st.nextInterval())
	go func() {
		for ctx.Err() == nil {
			select {
			case t := <-st.ticker.C:
				if i, s := sf(); i != slitIndex || s != totalSlots {
					slitIndex, totalSlots = i, s
					st.ticker.Reset(st.nextInterval())
					st.shouldReset = true
					continue
				}

				c <- t
				if st.shouldReset {
					st.ticker.Reset(st.d)
				}

				st.shouldReset = false
			case <-ctx.Done():
				return
			}
		}
		close(c)
	}()
	return st
}

func (t *SlottedTicker) Stop() {
	t.ticker.Stop()
	t.done()
}

func (t *SlottedTicker) nextInterval() time.Duration {
	slotIndex, totalSlots := t.sf()

	// Discover what time the last iteration started
	lastStartTime := time.UnixMilli((time.Now().UnixMilli() / t.d.Milliseconds()) * t.d.Milliseconds())
	offset := time.Duration((float64(slotIndex) / float64(totalSlots)) * float64(t.d))
	// Lets offset the time of the iteration
	lastStartTime = lastStartTime.Add(offset)

	// Keep adding the ticker duration until we pass time.now
	for lastStartTime.Before(time.Now()) {
		lastStartTime = lastStartTime.Add(t.d)
	}

	slotSize := t.d / time.Duration(totalSlots)
	return time.Until(lastStartTime) + PositiveJitter(slotSize, t.slotJitter)
}

func ParseDurationMs(s string) (int64, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second/time.Millisecond)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration. It overflows int64", s)
		}
		return int64(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return int64(d) / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration", s)
}

func DurationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

// Copied from https://github.com/prometheus/prometheus/blob/dfae954dc1137568f33564e8cffda321f2867925/promql/engine.go#L811
func GetTimeRangesForSelector(start, end int64, lookbackDelta time.Duration, n *parser.VectorSelector, path []parser.Node, evalRange time.Duration) (int64, int64) {
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
		offsetMilliseconds := DurationMilliseconds(subqOffset)
		start = start - offsetMilliseconds - DurationMilliseconds(subqRange)
		end -= offsetMilliseconds
	}

	if evalRange == 0 {
		start -= DurationMilliseconds(lookbackDelta)
	} else {
		// For all matrix queries we want to ensure that we have (end-start) + range selected
		// this way we have `range` data before the start time
		start -= DurationMilliseconds(evalRange)
	}

	offsetMilliseconds := DurationMilliseconds(n.OriginalOffset)
	start -= offsetMilliseconds
	end -= offsetMilliseconds

	return start, end
}

// Copied from https://github.com/prometheus/prometheus/blob/dfae954dc1137568f33564e8cffda321f2867925/promql/engine.go#L754
// subqueryTimes returns the sum of offsets and ranges of all subqueries in the path.
// If the @ modifier is used, then the offset and range is w.r.t. that timestamp
// (i.e. the sum is reset when we have @ modifier).
// The returned *int64 is the closest timestamp that was seen. nil for no @ modifier.
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
