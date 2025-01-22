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

func dynamicIntervalFn(cfg Config, limits tripperware.Limits, queryAnalyzer querysharding.Analyzer, queryStoreAfter time.Duration, lookbackDelta time.Duration) func(ctx context.Context, r tripperware.Request) (time.Duration, error) {
	return func(ctx context.Context, r tripperware.Request) (time.Duration, error) {
		tenantIDs, err := tenant.TenantIDs(ctx)
		if err != nil {
			return cfg.SplitQueriesByInterval, err
		}

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

		// Calculates: duration of data fetched if the query was not sharded, the original range covered by the query start and end times,
		// and the duration of data fetched by lookbackDelta for the first split
		durationFetchedWithoutSharding, originalRangeCount, firstSplitLookbackDeltaCompensation := durationFetchedByQuery(queryExpr, r, queryStoreAfter, lookbackDelta, cfg.SplitQueriesByInterval, time.Now())
		extraDaysFetchedPerShard := durationFetchedWithoutSharding - originalRangeCount

		// Calculate the extra duration of data fetched by lookbackDelta per each split except the first split
		nextIntervalStart := nextIntervalBoundary(r.GetStart(), r.GetStep(), cfg.SplitQueriesByInterval) + r.GetStep()
		nextIntervalReq := r.WithStartEnd(nextIntervalStart, r.GetEnd())
		_, _, lookbackDeltaCompensation := durationFetchedByQuery(queryExpr, nextIntervalReq, queryStoreAfter, lookbackDelta, cfg.SplitQueriesByInterval, time.Now())

		var maxSplitsByFetchedDaysOfData int
		if cfg.DynamicQuerySplitsConfig.MaxDurationOfDataFetchedFromStoragePerQuery > 0 {
			if extraDaysFetchedPerShard == 0 {
				extraDaysFetchedPerShard = 1 // prevent division by 0
			}
			maxIntervalsFetchedByQuery := int(cfg.DynamicQuerySplitsConfig.MaxDurationOfDataFetchedFromStoragePerQuery / cfg.SplitQueriesByInterval)
			maxSplitsByFetchedDaysOfData = ((maxIntervalsFetchedByQuery / queryVerticalShardSize) - originalRangeCount + firstSplitLookbackDeltaCompensation) / (extraDaysFetchedPerShard + lookbackDeltaCompensation)
			if maxSplitsByFetchedDaysOfData <= 0 {
				maxSplitsByFetchedDaysOfData = 1
			}
		}

		var maxSplitsByConfig int
		if cfg.DynamicQuerySplitsConfig.MaxShardsPerQuery > 0 {
			maxSplitsByConfig = cfg.DynamicQuerySplitsConfig.MaxShardsPerQuery / queryVerticalShardSize
			if maxSplitsByConfig <= 0 {
				maxSplitsByConfig = 1
			}
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

		// Calculate the multiple of interval needed to shard query to <= maxSplits
		n1 := (queryRange + baseInterval*maxSplits - 1) / (baseInterval * maxSplits)
		if n1 <= 0 {
			n1 = 1
		}

		// The first split can be truncated and not cover the full length of n*interval.
		// So we remove it and calculate the multiple of interval needed to shard <= maxSplits-1
		nextSplitStart := nextIntervalBoundary(r.GetStart(), r.GetStep(), n1*baseInterval) + r.GetStep()
		queryRangeWithoutFirstSplit := time.Duration((r.GetEnd() - nextSplitStart) * int64(time.Millisecond))
		var n2 time.Duration
		if maxSplits > 1 {
			n2 = (queryRangeWithoutFirstSplit + baseInterval*(maxSplits-1) - 1) / (baseInterval * (maxSplits - 1))
		} else {
			// If maxSplits is <= 1 then we should not shard at all
			n1 += (queryRangeWithoutFirstSplit + baseInterval - 1) / baseInterval
		}
		n := max(n1, n2)
		return n * cfg.SplitQueriesByInterval, nil
	}
}

// calculates the total duration of data the query will have to fetch from storage as a multiple of baseInterval.
// also returns the total time range fetched by the original query start and end times
func durationFetchedByQuery(expr parser.Expr, req tripperware.Request, queryStoreAfter, lookbackDelta time.Duration, baseInterval time.Duration, now time.Time) (durationFetchedCount int, originalRangeCount int, lookbackDeltaCount int) {
	durationFetchedCount = 0
	originalRangeCount = 0
	lookbackDeltaCount = 0
	baseIntervalMillis := util.DurationMilliseconds(baseInterval)
	queryStoreMaxT := util.TimeToMillis(now.Add(-queryStoreAfter))
	var evalRange time.Duration

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			originalRangeCount += int((req.GetEnd()/baseIntervalMillis)-(req.GetStart()/baseIntervalMillis)) + 1
			start, end := util.GetTimeRangesForSelector(req.GetStart(), req.GetEnd(), 0, n, path, evalRange)
			// Query shouldn't touch Store Gateway.
			if start > queryStoreMaxT {
				return nil
			} else {
				// If the query split needs to query store, cap the max time to now - queryStoreAfter.
				end = min(end, queryStoreMaxT)
			}

			startIntervalIndex := start / baseIntervalMillis
			endIntervalIndex := end / baseIntervalMillis
			durationFetchedCount += int(endIntervalIndex-startIntervalIndex) + 1

			if evalRange == 0 && (start-util.DurationMilliseconds(lookbackDelta))/baseIntervalMillis == start/baseIntervalMillis {
				lookbackDeltaCount += 1
			}
			evalRange = 0
		case *parser.MatrixSelector:
			evalRange = n.Range
		}
		return nil
	})
	return durationFetchedCount, originalRangeCount, lookbackDeltaCount
}
