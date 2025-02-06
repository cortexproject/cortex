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
		maxSplitsConfigValue := cfg.DynamicQuerySplitsConfig.MaxSplitsPerQuery
		maxDurationFetchedConfigValue := cfg.DynamicQuerySplitsConfig.MaxFetchedStorageDataDurationPerQuery

		queryVerticalShardSize, err := getMaxVerticalShardSizeForQuery(ctx, r, limits, queryAnalyzer)
		if err != nil {
			return baseInterval, err
		}

		queryExpr, err := parser.ParseExpr(r.GetQuery())
		if err != nil {
			return baseInterval, err
		}

		// Get the max number of splits allowed by each of the two configs
		maxSplitsFromConfig := getMaxSplitsFromConfig(maxSplitsConfigValue, queryVerticalShardSize)
		maxSplitsFromDurationFetched := getMaxSplitsByDurationFetched(maxDurationFetchedConfigValue, queryVerticalShardSize, queryExpr, r.GetStart(), r.GetEnd(), r.GetStep(), baseInterval, lookbackDelta)

		// Use the more restrictive max splits limit
		var maxSplits int
		switch {
		case maxSplitsConfigValue > 0 && maxDurationFetchedConfigValue > 0:
			maxSplits = min(maxSplitsFromConfig, maxSplitsFromDurationFetched)
		case maxSplitsConfigValue > 0:
			maxSplits = maxSplitsFromConfig
		case maxDurationFetchedConfigValue > 0:
			maxSplits = maxSplitsFromDurationFetched
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

	for n <= 2*((queryRange+baseInterval-1)/baseInterval) {
		// The first split could be truncated and shorter than other splits.
		// So it is removed to check if a larger interval is needed
		nextSplitStart := nextIntervalBoundary(r.GetStart(), r.GetStep(), n*baseInterval) + r.GetStep()
		if maxSplits == 1 {
			// No splitting. Exit loop if first split is long enough to cover the full query range
			if nextSplitStart >= r.GetEnd() {
				break
			}
		} else {
			// Recalculate n for remaining query range after removing first split.
			// Exit loop if a larger n is not needed to split into <= maxSplits-1
			queryRangeWithoutFirstSplit := time.Duration((r.GetEnd() - nextSplitStart) * int64(time.Millisecond))
			n_temp := (queryRangeWithoutFirstSplit + baseInterval*(maxSplits-1) - 1) / (baseInterval * (maxSplits - 1))
			if n >= n_temp {
				break
			}
		}
		n++
	}
	return n * baseInterval
}

func getMaxSplitsFromConfig(maxSplitsConfigValue int, queryVerticalShardSize int) int {
	// Find max number of allowed splits by MaxSplitsPerQuery config after accounting for vertical sharding
	var maxSplitsFromConfig int
	if maxSplitsConfigValue > 0 {
		maxSplitsFromConfig = maxSplitsConfigValue / queryVerticalShardSize
	}
	if maxSplitsFromConfig <= 0 {
		maxSplitsFromConfig = 1
	}
	return maxSplitsFromConfig
}

func getMaxSplitsByDurationFetched(maxDurationFetchedConfigValue time.Duration, queryVerticalShardSize int, expr parser.Expr, queryStart int64, queryEnd int64, queryStep int64, baseInterval time.Duration, lookbackDelta time.Duration) int {
	// Get variables needed to calculate the max number of allowed splits by MaxFetchedStorageDataDurationPerQuery config
	intervalsFetchedByQueryRange, extraIntervalsFetchedPerSplit, intervalsFetchedByLookbackDeltaForFirstSplit, intervalsFetchedByLookbackDeltaForOtherSplits := getVariablesToCalculateMaxSplitsByDurationFetched(expr, queryStart, queryEnd, queryStep, baseInterval, lookbackDelta)

	// Handle different cases for lookbackDelta fetching additional duration of data
	if intervalsFetchedByLookbackDeltaForFirstSplit > 0 && intervalsFetchedByLookbackDeltaForOtherSplits > 0 {
		// lookbackDelta is fetching additional duration for all splits
		extraIntervalsFetchedPerSplit += intervalsFetchedByLookbackDeltaForOtherSplits
	} else if intervalsFetchedByLookbackDeltaForOtherSplits > 0 {
		// lookbackDelta is fetching additional duration for all splits except first one 
		extraIntervalsFetchedPerSplit += intervalsFetchedByLookbackDeltaForOtherSplits
		intervalsFetchedByQueryRange -= intervalsFetchedByLookbackDeltaForOtherSplits
	} else if intervalsFetchedByLookbackDeltaForFirstSplit > 0 {
		// lookbackDelta is fetching additional duration for first split only 
		intervalsFetchedByQueryRange += intervalsFetchedByLookbackDeltaForFirstSplit
	}

	var maxSplitsByDurationFetched int
	if maxDurationFetchedConfigValue > 0 {
		// Example equation for duration fetched by query up[15d:1h] with a range of 30 days, 1 day base split interval, and 5 min lookbackDelta
		// [duration_fetched_by_range + (extra_duration_fetched_per_split x horizontal_splits)] x vertical_shards <= maxDurationFetchedConfigValue
		// [30 + (16 x horizontal_splits)] x vertical_shards <= maxDurationFetchedConfigValue
		// Rearranging the equation to find the max horizontal splits
		maxDurationFetchedAsMultipleOfInterval := int(maxDurationFetchedConfigValue / baseInterval)
		maxSplitsByDurationFetched = ((maxDurationFetchedAsMultipleOfInterval / queryVerticalShardSize) - intervalsFetchedByQueryRange) / extraIntervalsFetchedPerSplit
	}
	if maxSplitsByDurationFetched <= 0 {
		maxSplitsByDurationFetched = 1
	}
	return maxSplitsByDurationFetched
}

func getVariablesToCalculateMaxSplitsByDurationFetched(expr parser.Expr, queryStart int64, queryEnd int64, queryStep int64, baseInterval time.Duration, lookbackDelta time.Duration) (intervalsFetchedByQueryRange int, extraIntervalsFetchedPerSplit int, intervalsFetchedByLookbackDeltaForFirstSplit int, intervalsFetchedByLookbackDeltaForOtherSplits int) {
	// First analyze the query using original start-end time. Additional duration fetched by lookbackDelta here only reflects the start time of first split
	intervalsFetchedByQueryRange, extraIntervalsFetchedPerSplit, intervalsFetchedByLookbackDeltaForFirstSplit = analyzeDurationFetchedByQueryExpr(expr, queryStart, queryEnd, baseInterval, lookbackDelta)
	if extraIntervalsFetchedPerSplit == 0 {
		extraIntervalsFetchedPerSplit = 1 // avoid division by 0
	}

	// Next analyze the query using the next split start time to find the additional duration fetched by lookbackDelta for splits other than first one
	nextIntervalStart := nextIntervalBoundary(queryStart, queryStep, baseInterval) + queryStep
	_, _, intervalsFetchedByLookbackDeltaForOtherSplits = analyzeDurationFetchedByQueryExpr(expr, nextIntervalStart, queryEnd, baseInterval, lookbackDelta)

	return intervalsFetchedByQueryRange, extraIntervalsFetchedPerSplit, intervalsFetchedByLookbackDeltaForFirstSplit, intervalsFetchedByLookbackDeltaForOtherSplits
}

// analyzeDurationFetchedByQueryExpr analyzes the query to identify variables useful for
// calculating the duration of data that will be fetched from storage when the query
// is executed after being split. All variables are expressed as an integer count of multiples
// of the base split interval.
//
// Returns:
//   - intervalsFetchedByQueryRange: The total number of intervals fetched by the original start-end
//     range of the query. This value does not depend on the number of splits.
//   - extraIntervalsFetchedPerSplit: The number of additional intervals fetched by matrix selectors
//     and/or subqueries. This duration will be fetched by every query split.
//   - intervalsFetchedByLookbackDelta: The number of additional intervals fetched by lookbackDelta
//     for the specified start time.
//
// Example:
// Query up[15d:1h] with a range of 30 days, 1 day base split interval, and 5 min lookbackDelta
// - intervalsFetchedByQueryRange = 30
// - extraIntervalsFetchedPerSplit = 15
// - intervalsFetchedByLookbackDelta = 1
func analyzeDurationFetchedByQueryExpr(expr parser.Expr, queryStart int64, queryEnd int64, baseInterval time.Duration, lookbackDelta time.Duration) (intervalsFetchedByQueryRange int, extraIntervalsFetchedPerSplit int, intervalsFetchedByLookbackDelta int) {
	intervalsFetchedByQueryRange = 0
	intervalsFetchedByLookbackDelta = 0
	baseIntervalMillis := util.DurationMilliseconds(baseInterval)

	totalIntervalsFetched := 0
	var evalRange time.Duration
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			// Increment intervals fetched by the original start-end time range
			queryStartIntervalIndex := floorDiv(queryStart, baseIntervalMillis)
			queryEndIntervalIndex := floorDiv(queryEnd, baseIntervalMillis)
			intervalsFetchedByQueryRange += int(queryEndIntervalIndex-queryStartIntervalIndex) + 1

			// Adjust start and end time based on matrix selectors or subquery and increment total intervals fetched, this excludes lookbackDelta
			start, end := util.GetTimeRangesForSelector(queryStart, queryEnd, 0, n, path, evalRange)
			startIntervalIndex := floorDiv(start, baseIntervalMillis)
			endIntervalIndex := floorDiv(end, baseIntervalMillis)
			totalIntervalsFetched += int(endIntervalIndex-startIntervalIndex) + 1

			// Increment intervals fetched by lookbackDelta
			startLookbackDelta := start - util.DurationMilliseconds(lookbackDelta)
			startLookbackDeltaIntervalIndex := floorDiv(startLookbackDelta, baseIntervalMillis)
			if evalRange == 0 && startLookbackDeltaIntervalIndex < startIntervalIndex {
				intervalsFetchedByLookbackDelta += int(startIntervalIndex - startLookbackDeltaIntervalIndex)
			}
			evalRange = 0
		case *parser.MatrixSelector:
			evalRange = n.Range
		}
		return nil
	})

	extraIntervalsFetchedPerSplit = totalIntervalsFetched - intervalsFetchedByQueryRange
	return intervalsFetchedByQueryRange, extraIntervalsFetchedPerSplit, intervalsFetchedByLookbackDelta
}

func floorDiv(a, b int64) int64 {
	if a < 0 && a%b != 0 {
		return a/b - 1
	}
	return a / b
}
