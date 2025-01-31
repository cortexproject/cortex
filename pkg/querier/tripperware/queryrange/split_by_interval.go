package queryrange

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/httpgrpc"

	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type IntervalFn func(ctx context.Context, r tripperware.Request) (time.Duration, error)

// SplitByIntervalMiddleware creates a new Middleware that splits requests by a given interval.
func SplitByIntervalMiddleware(interval IntervalFn, limits tripperware.Limits, merger tripperware.Merger, registerer prometheus.Registerer, lookbackDelta time.Duration) tripperware.Middleware {
	return tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
		return splitByInterval{
			next:     next,
			limits:   limits,
			merger:   merger,
			interval: interval,
			splitByCounter: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Namespace: "cortex",
				Name:      "frontend_split_queries_total",
				Help:      "Total number of underlying query requests after the split by interval is applied",
			}),
			lookbackDelta: lookbackDelta,
		}
	})
}

type splitByInterval struct {
	next          tripperware.Handler
	limits        tripperware.Limits
	merger        tripperware.Merger
	interval      IntervalFn
	lookbackDelta time.Duration

	// Metrics.
	splitByCounter prometheus.Counter
}

func (s splitByInterval) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	// First we're going to build new requests, one for each day, taking care
	// to line up the boundaries with step.
	interval, err := s.interval(ctx, r)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, err.Error())
	}
	reqs, err := splitQuery(r, interval)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}
	s.splitByCounter.Add(float64(len(reqs)))

	stats := querier_stats.FromContext(ctx)
	if stats != nil {
		stats.SplitInterval = interval
	}
	reqResps, err := tripperware.DoRequests(ctx, s.next, reqs, s.limits)
	if err != nil {
		return nil, err
	}

	resps := make([]tripperware.Response, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.Response)
	}

	response, err := s.merger.MergeResponse(ctx, nil, resps...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func splitQuery(r tripperware.Request, interval time.Duration) ([]tripperware.Request, error) {
	// If Start == end we should just run the original request
	if r.GetStart() == r.GetEnd() {
		return []tripperware.Request{r}, nil
	}

	// Replace @ modifier function to their respective constant values in the query.
	// This way subqueries will be evaluated at the same time as the parent query.
	query, err := evaluateAtModifierFunction(r.GetQuery(), r.GetStart(), r.GetEnd())
	if err != nil {
		return nil, err
	}
	var reqs []tripperware.Request
	for start := r.GetStart(); start < r.GetEnd(); start = nextIntervalBoundary(start, r.GetStep(), interval) + r.GetStep() {
		end := nextIntervalBoundary(start, r.GetStep(), interval)
		if end+r.GetStep() >= r.GetEnd() {
			end = r.GetEnd()
		}

		reqs = append(reqs, r.WithQuery(query).WithStartEnd(start, end))
	}
	return reqs, nil
}

// evaluateAtModifierFunction parse the query and evaluates the `start()` and `end()` at modifier functions into actual constant timestamps.
// For example given the start of the query is 10.00, `http_requests_total[1h] @ start()` query will be replaced with `http_requests_total[1h] @ 10.00`
// If the modifier is already a constant, it will be returned as is.
func evaluateAtModifierFunction(query string, start, end int64) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", httpgrpc.Errorf(http.StatusBadRequest, "%s", err)
	}
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		if selector, ok := n.(*parser.VectorSelector); ok {
			switch selector.StartOrEnd {
			case parser.START:
				selector.Timestamp = &start
			case parser.END:
				selector.Timestamp = &end
			}
			selector.StartOrEnd = 0
		}
		if selector, ok := n.(*parser.SubqueryExpr); ok {
			switch selector.StartOrEnd {
			case parser.START:
				selector.Timestamp = &start
			case parser.END:
				selector.Timestamp = &end
			}
			selector.StartOrEnd = 0
		}
		return nil
	})
	return expr.String(), err
}

// Round up to the step before the next interval boundary.
func nextIntervalBoundary(t, step int64, interval time.Duration) int64 {
	msPerInterval := int64(interval / time.Millisecond)
	startOfNextInterval := ((t / msPerInterval) + 1) * msPerInterval
	// ensure that target is a multiple of steps away from the start time
	target := startOfNextInterval - ((startOfNextInterval - t) % step)
	if target == startOfNextInterval {
		target -= step
	}
	return target
}

func staticIntervalFn(cfg Config) func(ctx context.Context, r tripperware.Request) (time.Duration, error) {
	return func(_ context.Context, _ tripperware.Request) (time.Duration, error) {
		return cfg.SplitQueriesByInterval, nil
	}
}

func dynamicIntervalFn(cfg Config, limits tripperware.Limits, queryAnalyzer querysharding.Analyzer, lookbackDelta time.Duration) func(ctx context.Context, r tripperware.Request) (time.Duration, error) {
	return func(ctx context.Context, r tripperware.Request) (time.Duration, error) {
		baseInterval := cfg.SplitQueriesByInterval
		maxDurationFetchedConfig := cfg.DynamicQuerySplitsConfig.MaxFetchedStorageDataDurationPerQuery
		maxSplitsConfig := cfg.DynamicQuerySplitsConfig.MaxSplitsPerQuery

		queryVerticalShardSize, err := getMaxVerticalShardSizeForQuery(ctx, r, limits, queryAnalyzer)
		if err != nil {
			return baseInterval, err
		}

		queryExpr, err := parser.ParseExpr(r.GetQuery())
		if err != nil {
			return baseInterval, err
		}

		// First analyze the query using original start-end time. Additional duration fetched by lookbackDelta here only reflects the start time of first split
		queryRangeIntervals, extraIntervalsPerSplit, firstSplitLookbackDeltaIntervals := analyzeDurationFetchedByQuery(queryExpr, r.GetStart(), r.GetEnd(), baseInterval, lookbackDelta)
		if extraIntervalsPerSplit == 0 {
			extraIntervalsPerSplit = 1 // avoid division by 0
		}

		// Next analyze the query using the next split start time to find the additional duration fetched by lookbackDelta for other subsequent splits
		nextIntervalStart := nextIntervalBoundary(r.GetStart(), r.GetStep(), baseInterval) + r.GetStep()
		_, _, otherSplitsLookbackDeltaIntervals := analyzeDurationFetchedByQuery(queryExpr, nextIntervalStart, r.GetEnd(), baseInterval, lookbackDelta)

		// By default subtract the 'first split' duration fetched by loookbackDelta, and divide by the 'other splits' duration fetched by loookbackDelta.
		if firstSplitLookbackDeltaIntervals > 0 && otherSplitsLookbackDeltaIntervals > 0 {
			firstSplitLookbackDeltaIntervals = 0 // Dividing is enough if additional duration is fetched by loookbackDelta for all splits
		} else if otherSplitsLookbackDeltaIntervals > 0 {
			firstSplitLookbackDeltaIntervals = otherSplitsLookbackDeltaIntervals * -1 // Adding instead of subtracting for first split, if additional duration is fetched by loookbackDelta for all splits except first one
		}

		// Find the max number of splits that will fetch less than MaxFetchedStorageDataDurationPerQuery
		var maxSplitsByDurationFetched int
		if maxDurationFetchedConfig > 0 {
			maxIntervalsFetchedByQuery := int(maxDurationFetchedConfig / baseInterval)
			// Example equation for duration fetched by query: up[15d:1h] with a range of 30 days, a base split interval of 24 hours, and 5 min lookbackDelta
			// MaxFetchedStorageDataDurationPerQuery > (30 + ((15 + 1) x horizontal splits)) x vertical shards
			// Rearranging the equation to find the max horizontal splits
			maxSplitsByDurationFetched = ((maxIntervalsFetchedByQuery / queryVerticalShardSize) - queryRangeIntervals - firstSplitLookbackDeltaIntervals) / (extraIntervalsPerSplit + otherSplitsLookbackDeltaIntervals)
			if maxSplitsByDurationFetched <= 0 {
				maxSplitsByDurationFetched = 1
			}
		}

		// Find max number of splits from MaxSplitsPerQuery after accounting for vertical sharding
		var maxSplitsByConfig int
		if maxSplitsConfig > 0 {
			maxSplitsByConfig = maxSplitsConfig / queryVerticalShardSize
			if maxSplitsByConfig <= 0 {
				maxSplitsByConfig = 1
			}
		}

		var maxSplits int
		switch {
		case maxDurationFetchedConfig > 0 && maxSplitsConfig > 0:
			// Use the more restricting shard limit
			maxSplits = min(maxSplitsByConfig, maxSplitsByDurationFetched)
		case maxSplitsConfig > 0:
			maxSplits = maxSplitsByConfig
		case maxDurationFetchedConfig > 0:
			maxSplits = maxSplitsByDurationFetched
		default:
			return baseInterval, nil
		}

		interval := getIntervalFromMaxSplits(r, baseInterval, maxSplits)
		return interval, nil
	}
}

func getMaxVerticalShardSizeForQuery(ctx context.Context, r tripperware.Request, limits tripperware.Limits, queryAnalyzer querysharding.Analyzer) (int, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return 1, err
	}

	analysis, err := queryAnalyzer.Analyze(r.GetQuery())
	if err != nil {
		return 1, err
	}

	queryVerticalShardSize := validation.SmallestPositiveIntPerTenant(tenantIDs, limits.QueryVerticalShardSize)
	if queryVerticalShardSize <= 0 || !analysis.IsShardable() {
		queryVerticalShardSize = 1
	}
	return queryVerticalShardSize, nil
}

func getIntervalFromMaxSplits(r tripperware.Request, baseInterval time.Duration, maxSplitsInt int) time.Duration {
	maxSplits := time.Duration(maxSplitsInt)
	queryRange := time.Duration((r.GetEnd() - r.GetStart()) * int64(time.Millisecond))

	// Calculate the multiple n of interval needed to shard query to <= maxSplits
	n := (queryRange + baseInterval*maxSplits - 1) / (baseInterval * maxSplits)
	if n <= 0 {
		n = 1
	}

	if maxSplits == 1 {
		// No splitting, interval should be long enough to result in 1 split only
		nextSplitStart := nextIntervalBoundary(r.GetStart(), r.GetStep(), n*baseInterval) + r.GetStep()
		if nextSplitStart < r.GetEnd() {
			queryRangeWithoutFirstSplit := time.Duration((r.GetEnd() - nextSplitStart) * int64(time.Millisecond))
			n += (queryRangeWithoutFirstSplit + baseInterval - 1) / baseInterval
		}
	} else {
		for n <= 2*((queryRange+baseInterval-1)/baseInterval) {
			// The first split can be truncated and shorter than other splits.
			// So it is removed to check if a larger interval is needed to shard <= maxSplits-1
			nextSplitStart := nextIntervalBoundary(r.GetStart(), r.GetStep(), n*baseInterval) + r.GetStep()
			queryRangeWithoutFirstSplit := time.Duration((r.GetEnd() - nextSplitStart) * int64(time.Millisecond))
			n_temp := (queryRangeWithoutFirstSplit + baseInterval*(maxSplits-1) - 1) / (baseInterval * (maxSplits - 1))
			if n >= n_temp {
				break
			}
			n++
		}
	}
	return n * baseInterval
}

// analyzeDurationFetchedByQuery analyzes the query to identify variables useful for
// calculating the duration of data that will be fetched from storage when the query
// is executed after being split. All variables are expressed as a count of multiples
// of the base split interval.
//
// Returns:
//   - queryRangeIntervals: The total number of intervals fetched by the original start-end
//     range of the query. This value is constant and does not depend on the number of splits.
//   - extraIntervalsPerSplit: The number of additional intervals fetched by matrix selectors
//     or subqueries. This value will be fetched once for every split.
//   - lookbackDeltaIntervals: The number of additional intervals fetched by the lookbackDelta
//     for the specified start time.
//
// Example:
// Query up[15d:1h] with a range of 30 days, a base split interval of 24 hours, and 5 min lookbackDelta
// - queryRangeIntervals = 30
// - extraIntervalsPerSplit = 15
// - lookbackDeltaIntervals = 1
func analyzeDurationFetchedByQuery(expr parser.Expr, queryStart int64, queryEnd int64, baseInterval time.Duration, lookbackDelta time.Duration) (queryRangeIntervals int, extraIntervalsPerSplit int, lookbackDeltaIntervals int) {
	queryRangeIntervals = 0
	lookbackDeltaIntervals = 0
	baseIntervalMillis := util.DurationMilliseconds(baseInterval)

	totalDurationFetched := 0
	var evalRange time.Duration
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			// Increment  of intervals fetched by the original start-end time range
			queryRangeIntervals += int((queryEnd/baseIntervalMillis)-(queryStart/baseIntervalMillis)) + 1

			// Adjust start and end time based on matrix selectors or subquery, this excludes lookbackDelta
			start, end := util.GetTimeRangesForSelector(queryStart, queryEnd, 0, n, path, evalRange)
			startIntervalIndex := floorDiv(start, baseIntervalMillis)
			endIntervalIndex := floorDiv(end, baseIntervalMillis)
			totalDurationFetched += int(endIntervalIndex-startIntervalIndex) + 1

			// Adjust start time based on lookbackDelta and increment the additional  of intervals fetched by it
			startLookbackDelta := start - util.DurationMilliseconds(lookbackDelta)
			startLookbackDeltaIntervalIndex := floorDiv(startLookbackDelta, baseIntervalMillis)
			if evalRange == 0 && startLookbackDeltaIntervalIndex < startIntervalIndex {
				lookbackDeltaIntervals += int(startIntervalIndex - startLookbackDeltaIntervalIndex)
			}
			evalRange = 0
		case *parser.MatrixSelector:
			evalRange = n.Range
		}
		return nil
	})
	extraIntervalsPerSplit = totalDurationFetched - queryRangeIntervals

	return queryRangeIntervals, extraIntervalsPerSplit, lookbackDeltaIntervals
}

func floorDiv(a, b int64) int64 {
	if a < 0 && a%b != 0 {
		return a/b - 1
	}
	return a / b
}
