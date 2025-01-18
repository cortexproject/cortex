package queryrange

import (
	"context"
	"net/http"
	"sort"
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

// dayMillis is the L4 block range in milliseconds.
var dayMillis = util.DurationMilliseconds(24 * time.Hour)

type IntervalFn func(ctx context.Context, r tripperware.Request) (time.Duration, error)

type dayRange struct {
	startDay int64
	endDay   int64
}

// SplitByIntervalMiddleware creates a new Middleware that splits requests by a given interval.
func SplitByIntervalMiddleware(interval IntervalFn, limits tripperware.Limits, merger tripperware.Merger, registerer prometheus.Registerer, queryStoreAfter time.Duration, lookbackDelta time.Duration) tripperware.Middleware {
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
			queryStoreAfter: queryStoreAfter,
			lookbackDelta:   lookbackDelta,
		}
	})
}

type splitByInterval struct {
	next     tripperware.Handler
	limits   tripperware.Limits
	merger   tripperware.Merger
	interval IntervalFn

	// Metrics.
	splitByCounter prometheus.Counter

	queryStoreAfter time.Duration
	lookbackDelta   time.Duration
}

func (s splitByInterval) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	// First we're going to build new requests, one for each day, taking care
	// to line up the boundaries with step.
	interval, err := s.interval(ctx, r)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
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

func dynamicIntervalFn(cfg Config, limits tripperware.Limits, queryAnalyzer querysharding.Analyzer, queryStoreAfter time.Duration, lookbackDelta time.Duration, maxDaysOfDataFetched int) func(ctx context.Context, r tripperware.Request) (time.Duration, error) {
	return func(ctx context.Context, r tripperware.Request) (time.Duration, error) {
		tenantIDs, err := tenant.TenantIDs(ctx)
		if err != nil {
			return cfg.SplitQueriesByInterval, err
		}

		queryDayRange := int((r.GetEnd() / dayMillis) - (r.GetStart() / dayMillis) + 1)
		analysis, err := queryAnalyzer.Analyze(r.GetQuery())
		if err != nil {
			return cfg.SplitQueriesByInterval, err
		}

		queryVerticalShardSize := validation.SmallestPositiveIntPerTenant(tenantIDs, limits.QueryVerticalShardSize)
		if queryVerticalShardSize <= 0 || !analysis.IsShardable() {
			queryVerticalShardSize = 1
		}

		queryExpr, err := parser.ParseExpr(r.GetQuery())
		if err != nil {
			return cfg.SplitQueriesByInterval, err
		}
		daysFetchedWithoutSharding := getDaysFetchedByQuery(queryExpr, []tripperware.Request{r}, queryStoreAfter, lookbackDelta, time.Now())
		extraDaysFetchedPerShard := daysFetchedWithoutSharding - queryDayRange

		// if lookbackDelta is configured and the query start time is not 00:00 UTC, we need to account for 1 fetched day of data per split except for the first split
		lookbackDeltaCompensation := 0
		if lookbackDelta > 0 && (r.GetStart()-util.DurationMilliseconds(lookbackDelta))/dayMillis == r.GetStart()/dayMillis {
			lookbackDeltaCompensation = 1
		}

		var maxSplitsByFetchedDaysOfData int
		if maxDaysOfDataFetched > 0 {
			maxSplitsByFetchedDaysOfData = ((maxDaysOfDataFetched / queryVerticalShardSize) - queryDayRange - lookbackDeltaCompensation) / (extraDaysFetchedPerShard + lookbackDeltaCompensation)
		}

		var maxSplitsByConfig int
		if cfg.SplitQueriesByIntervalMaxSplits > 0 {
			maxSplitsByConfig = cfg.SplitQueriesByIntervalMaxSplits / queryVerticalShardSize
		}

		var maxSplits time.Duration
		switch {
		case maxSplitsByFetchedDaysOfData <= 0 && maxSplitsByConfig <= 0:
			return cfg.SplitQueriesByInterval, nil
		case maxSplitsByFetchedDaysOfData <= 0:
			maxSplits = time.Duration(maxSplitsByConfig)
		case maxSplitsByConfig <= 0:
			maxSplits = time.Duration(maxSplitsByFetchedDaysOfData)
		default:
			// Use the more restricting shard limit
			maxSplits = time.Duration(min(maxSplitsByConfig, maxSplitsByFetchedDaysOfData))
		}

		queryRange := time.Duration((r.GetEnd() - r.GetStart()) * int64(time.Millisecond))
		baseInterval := cfg.SplitQueriesByInterval
		n := (queryRange + baseInterval*maxSplits - 1) / (baseInterval * maxSplits)
		return n * cfg.SplitQueriesByInterval, nil
	}
}

// calculates the total number of days the query will have to fetch during execution, considering the query itself,
// queryStoreAfter and lookbackDelta.
func getDaysFetchedByQuery(expr parser.Expr, reqs []tripperware.Request, queryStoreAfter, lookbackDelta time.Duration, now time.Time) int {
	count := 0
	queryStoreMaxT := util.TimeToMillis(now.Add(-queryStoreAfter))
	var evalRange time.Duration

	for _, req := range reqs {
		var ranges []dayRange
		parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
			switch n := node.(type) {
			case *parser.VectorSelector:
				start, end := util.GetTimeRangesForSelector(req.GetStart(), req.GetEnd(), lookbackDelta, n, path, evalRange)
				// Query shouldn't touch Store Gateway.
				if start > queryStoreMaxT {
					return nil
				} else {
					// If the query split needs to query store, cap the max time to now - queryStoreAfter.
					end = min(end, queryStoreMaxT)
				}

				startDay := start / dayMillis
				endDay := end / dayMillis
				ranges = append(ranges, dayRange{startDay: startDay, endDay: endDay})
				evalRange = 0
			case *parser.MatrixSelector:
				evalRange = n.Range
			}
			return nil
		})
		nonOverlappingRanges := mergeDayRanges(ranges)
		for _, dayRange := range nonOverlappingRanges {
			count += int(dayRange.endDay-dayRange.startDay) + 1
		}
	}
	return count
}

func mergeDayRanges(ranges []dayRange) []dayRange {
	if len(ranges) == 0 {
		return ranges
	}

	// Sort ranges by their startDay
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].startDay < ranges[j].startDay
	})

	// Merge overlapping ranges
	merged := []dayRange{ranges[0]}
	for _, current := range ranges {
		last := &merged[len(merged)-1]
		if current.startDay <= last.endDay {
			if current.endDay > last.endDay {
				last.endDay = current.endDay
			}
		} else {
			merged = append(merged, current)
		}
	}
	return merged
}
