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

type IntervalFn func(ctx context.Context, r tripperware.Request) (context.Context, time.Duration, error)

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
	ctx, interval, err := s.interval(ctx, r)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}
	reqs, err := splitQuery(r, interval)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
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

// Returns a fixed split interval
func staticIntervalFn(cfg Config) func(ctx context.Context, r tripperware.Request) (context.Context, time.Duration, error) {
	return func(ctx context.Context, _ tripperware.Request) (context.Context, time.Duration, error) {
		return ctx, cfg.SplitQueriesByInterval, nil
	}
}

// Returns a dynamic multiple of base interval adjusted depending on configured 'max_shards_per_query' and 'max_fetched_data_duration_per_query'
func dynamicIntervalFn(cfg Config, limits tripperware.Limits, queryAnalyzer querysharding.Analyzer, lookbackDelta time.Duration) func(ctx context.Context, r tripperware.Request) (context.Context, time.Duration, error) {
	return func(ctx context.Context, r tripperware.Request) (context.Context, time.Duration, error) {
		baseInterval := cfg.SplitQueriesByInterval
		dynamicSplitCfg := cfg.DynamicQuerySplitsConfig
		if dynamicSplitCfg.MaxShardsPerQuery == 0 && dynamicSplitCfg.MaxFetchedDataDurationPerQuery == 0 {
			return ctx, baseInterval, nil
		}

		queryExpr, err := parser.ParseExpr(r.GetQuery())
		if err != nil {
			return ctx, baseInterval, err
		}

		maxVerticalShardSize, isShardable, err := getMaxVerticalShardSize(ctx, r, limits, queryAnalyzer)
		if err != nil {
			return ctx, baseInterval, err
		}

		minVerticalShardSize := maxVerticalShardSize
		if dynamicSplitCfg.EnableDynamicVerticalSharding {
			minVerticalShardSize = 1
		}

		interval := baseInterval
		verticalShardSize := maxVerticalShardSize
		totalShards := 0
		// Find the combination of horizontal splits and vertical shards that will result in the largest total number of shards
		for candidateVerticalShardSize := minVerticalShardSize; candidateVerticalShardSize <= maxVerticalShardSize; candidateVerticalShardSize++ {
			maxSplitsFromMaxShards := getMaxSplitsFromMaxQueryShards(dynamicSplitCfg.MaxShardsPerQuery, candidateVerticalShardSize)
			maxSplitsFromDurationFetched := getMaxSplitsFromDurationFetched(dynamicSplitCfg.MaxFetchedDataDurationPerQuery, candidateVerticalShardSize, queryExpr, r.GetStart(), r.GetEnd(), r.GetStep(), baseInterval, lookbackDelta)

			// Use the more restrictive max splits limit
			var maxSplits int
			switch {
			case dynamicSplitCfg.MaxShardsPerQuery > 0 && dynamicSplitCfg.MaxFetchedDataDurationPerQuery > 0:
				maxSplits = min(maxSplitsFromMaxShards, maxSplitsFromDurationFetched)
			case dynamicSplitCfg.MaxShardsPerQuery > 0:
				maxSplits = maxSplitsFromMaxShards
			case dynamicSplitCfg.MaxFetchedDataDurationPerQuery > 0:
				maxSplits = maxSplitsFromDurationFetched
			}

			candidateInterval := getIntervalFromMaxSplits(r, baseInterval, maxSplits)
			if candidateTotalShards := getExpectedTotalShards(r.GetStart(), r.GetEnd(), candidateInterval, candidateVerticalShardSize); candidateTotalShards > totalShards {
				interval = candidateInterval
				verticalShardSize = candidateVerticalShardSize
				totalShards = candidateTotalShards
			}
		}

		// Set number of vertical shards to be used in shard_by middleware
		if isShardable && maxVerticalShardSize > 1 {
			ctx = tripperware.InjectVerticalShardSizeToContext(ctx, verticalShardSize)
		}

		return ctx, interval, nil
	}
}

func getMaxVerticalShardSize(ctx context.Context, r tripperware.Request, limits tripperware.Limits, queryAnalyzer querysharding.Analyzer) (int, bool, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return 1, false, err
	}

	analysis, err := queryAnalyzer.Analyze(r.GetQuery())
	if err != nil {
		return 1, false, err
	}

	queryVerticalShardSize := validation.SmallestPositiveIntPerTenant(tenantIDs, limits.QueryVerticalShardSize)
	if queryVerticalShardSize <= 0 || !analysis.IsShardable() {
		queryVerticalShardSize = 1
	}
	return queryVerticalShardSize, analysis.IsShardable(), nil
}

// Returns the minimum multiple of base interval needed to split query into less than maxSplits
func getIntervalFromMaxSplits(r tripperware.Request, baseInterval time.Duration, maxSplitsInt int) time.Duration {
	maxSplits := time.Duration(maxSplitsInt)
	queryRange := time.Duration((r.GetEnd() - r.GetStart()) * int64(time.Millisecond))
	// Calculate the multiple (n) of base interval needed to shard query into <= maxSplits
	n := ceilDiv(int64(queryRange), int64(baseInterval*maxSplits))
	if n <= 0 {
		n = 1
	}

	// Loop to handle cases where first split is truncated and shorter than remaining splits.
	// Exits loop if interval (n) is sufficient after removing first split
	// If no suitable interval was found terminates at a maximum of interval = 2 * query range
	for n <= 2*ceilDiv(int64(queryRange), int64(baseInterval)) {
		// Find the new start time for query after removing first split
		nextSplitStart := nextIntervalBoundary(r.GetStart(), r.GetStep(), time.Duration(n)*baseInterval) + r.GetStep()
		if maxSplits == 1 {
			// If maxSplits == 1, the removed first split should cover the full query range.
			if nextSplitStart >= r.GetEnd() {
				break
			}
		} else {
			queryRangeWithoutFirstSplit := time.Duration((r.GetEnd() - nextSplitStart) * int64(time.Millisecond))
			// Recalculate n for the remaining query range with maxSplits-1.
			n_temp := ceilDiv(int64(queryRangeWithoutFirstSplit), int64(baseInterval*(maxSplits-1)))
			// If a larger interval is needed after removing the first split, the initial n was insufficient.
			if n >= n_temp {
				break
			}
		}
		// Increment n to check if larger interval fits the maxSplits constraint.
		n++
	}
	return time.Duration(n) * baseInterval
}

// Return max allowed number of splits by MaxShardsPerQuery config after accounting for vertical sharding
func getMaxSplitsFromMaxQueryShards(maxSplitsConfigValue int, queryVerticalShardSize int) int {
	var maxSplitsFromConfig int
	if maxSplitsConfigValue > 0 {
		maxSplitsFromConfig = maxSplitsConfigValue / queryVerticalShardSize
	}
	if maxSplitsFromConfig <= 0 {
		maxSplitsFromConfig = 1
	}
	return maxSplitsFromConfig
}

// Return max allowed number of splits by MaxFetchedDataDurationPerQuery config after accounting for vertical sharding
func getMaxSplitsFromDurationFetched(maxFetchedDataDurationPerQuery time.Duration, queryVerticalShardSize int, expr parser.Expr, queryStart int64, queryEnd int64, queryStep int64, baseInterval time.Duration, lookbackDelta time.Duration) int {
	durationFetchedByRange, durationFetchedBySelectors := analyzeDurationFetchedByQueryExpr(expr, queryStart, queryEnd, baseInterval, lookbackDelta)

	if durationFetchedBySelectors == 0 {
		return int(maxFetchedDataDurationPerQuery / baseInterval) // The total duration fetched does not increase with number of splits, return default max splits
	}

	var maxSplitsByDurationFetched int
	if maxFetchedDataDurationPerQuery > 0 {
		// Duration fetched by query after splitting = durationFetchedByRange + durationFetchedBySelectors x numOfShards
		// Rearranging the equation to find the max horizontal splits after accounting for vertical shards
		maxSplitsByDurationFetched = int(((maxFetchedDataDurationPerQuery / time.Duration(queryVerticalShardSize)) - durationFetchedByRange) / durationFetchedBySelectors)
	}
	if maxSplitsByDurationFetched <= 0 {
		maxSplitsByDurationFetched = 1
	}
	return maxSplitsByDurationFetched
}

// analyzeDurationFetchedByQueryExpr analyzes the query to calculate
// the estimated duration of data that will be fetched from storage
//
// Example:
// Query up[15d:1h] with a range of 30 days, 1 day base split interval, and 5 min lookbackDelta with 00:00 UTC start time
// - durationFetchedByRange = 30 day
// - durationFetchedBySelectors = 16 day
func analyzeDurationFetchedByQueryExpr(expr parser.Expr, queryStart int64, queryEnd int64, baseInterval time.Duration, lookbackDelta time.Duration) (durationFetchedByRange time.Duration, durationFetchedBySelectors time.Duration) {
	baseIntervalMillis := util.DurationMilliseconds(baseInterval)
	durationFetchedByRangeCount := 0
	durationFetchedBySelectorsCount := 0

	var evalRange time.Duration
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			// Increment duration fetched by the original start-end time range
			queryStartIntervalIndex := floorDiv(queryStart, baseIntervalMillis)
			queryEndIntervalIndex := floorDiv(queryEnd, baseIntervalMillis)
			durationFetchedByRangeCount += int(queryEndIntervalIndex-queryStartIntervalIndex) + 1

			// Adjust start time based on matrix selectors and/or subquery selectors and calculate additional lookback duration fetched
			start, end := util.GetTimeRangesForSelector(queryStart, queryEnd, lookbackDelta, n, path, evalRange)
			durationFetchedBySelectors := (end - start) - (queryEnd - queryStart)
			durationFetchedBySelectorsCount += int(ceilDiv(durationFetchedBySelectors, baseIntervalMillis))

			evalRange = 0
		case *parser.MatrixSelector:
			evalRange = n.Range
		}
		return nil
	})

	return time.Duration(durationFetchedByRangeCount) * baseInterval, time.Duration(durationFetchedBySelectorsCount) * baseInterval
}

func getExpectedTotalShards(queryStart int64, queryEnd int64, interval time.Duration, verticalShardSize int) int {
	queryRange := time.Duration((queryEnd - queryStart) * int64(time.Millisecond))
	expectedSplits := int(ceilDiv(int64(queryRange), int64(interval)))
	return expectedSplits * verticalShardSize
}

func floorDiv(a, b int64) int64 {
	if a < 0 && a%b != 0 {
		return a/b - 1
	}
	return a / b
}

func ceilDiv(a, b int64) int64 {
	if a > 0 && a%b != 0 {
		return a/b + 1
	}
	return a / b
}
