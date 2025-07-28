package tripperware

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/parser"
	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	cquerysharding "github.com/cortexproject/cortex/pkg/querysharding"
	"github.com/cortexproject/cortex/pkg/tenant"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	stop = errors.New("stop")
)

func ShardByMiddleware(logger log.Logger, limits Limits, merger Merger, queryAnalyzer querysharding.Analyzer) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return shardBy{
			next:     next,
			limits:   limits,
			merger:   merger,
			logger:   logger,
			analyzer: queryAnalyzer,
		}
	})
}

type shardBy struct {
	next     Handler
	limits   Limits
	logger   log.Logger
	merger   Merger
	analyzer querysharding.Analyzer
}

func (s shardBy) Do(ctx context.Context, r Request) (Response, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	stats := querier_stats.FromContext(ctx)

	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	maxVerticalShardSize := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limits.QueryVerticalShardSize)

	// Check if vertical shard size is set by dynamic query splitting
	verticalShardSize := maxVerticalShardSize
	if dynamicVerticalShardSize, ok := VerticalShardSizeFromContext(ctx); ok {
		verticalShardSize = dynamicVerticalShardSize
	}

	if verticalShardSize <= 1 {
		return s.next.Do(ctx, r)
	}

	logger := util_log.WithContext(ctx, s.logger)
	analysis, err := s.analyzer.Analyze(r.GetQuery())
	if err != nil {
		level.Warn(logger).Log("msg", "error analyzing query", "q", r.GetQuery(), "err", err)
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	stats.AddExtraFields(
		"shard_by.is_shardable", analysis.IsShardable(),
		"shard_by.max_num_shards", maxVerticalShardSize,
		"shard_by.num_shards", verticalShardSize,
		"shard_by.sharding_labels", analysis.ShardingLabels(),
	)

	if !analysis.IsShardable() {
		return s.next.Do(ctx, r)
	}

	reqs := s.shardQuery(logger, verticalShardSize, r, analysis)

	reqResps, err := DoRequests(ctx, s.next, reqs, s.limits)
	if err != nil {
		return nil, err
	}

	resps := make([]Response, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.Response)
	}

	return s.merger.MergeResponse(ctx, r, resps...)
}

func (s shardBy) shardQuery(l log.Logger, verticalShardSize int, r Request, analysis querysharding.QueryAnalysis) []Request {
	reqs := make([]Request, verticalShardSize)
	for i := 0; i < verticalShardSize; i++ {
		q, err := cquerysharding.InjectShardingInfo(r.GetQuery(), &storepb.ShardInfo{
			TotalShards: int64(verticalShardSize),
			ShardIndex:  int64(i),
			By:          analysis.ShardBy(),
			Labels:      analysis.ShardingLabels(),
		})
		reqs[i] = r.WithQuery(q)

		if err != nil {
			level.Warn(l).Log("msg", "error sharding query", "q", r.GetQuery(), "err", err)
			return []Request{r}
		}
	}

	return reqs
}

type verticalShardsKey struct{}

func VerticalShardSizeFromContext(ctx context.Context) (int, bool) {
	val := ctx.Value(verticalShardsKey{})
	if val == nil {
		return 1, false
	}
	verticalShardSize, ok := val.(int)
	return verticalShardSize, ok
}

func InjectVerticalShardSizeToContext(ctx context.Context, verticalShardSize int) context.Context {
	return context.WithValue(ctx, verticalShardsKey{}, verticalShardSize)
}

type disableBinaryExpressionAnalyzer struct {
	analyzer querysharding.Analyzer
}

func NewDisableBinaryExpressionAnalyzer(analyzer querysharding.Analyzer) *disableBinaryExpressionAnalyzer {
	return &disableBinaryExpressionAnalyzer{analyzer: analyzer}
}

func (d *disableBinaryExpressionAnalyzer) Analyze(query string) (querysharding.QueryAnalysis, error) {
	analysis, err := d.analyzer.Analyze(query)
	if err != nil || !analysis.IsShardable() {
		return analysis, err
	}

	expr, _ := parser.ParseExpr(query)
	isShardable := true
	promqlparser.Inspect(expr, func(node promqlparser.Node, nodes []promqlparser.Node) error {
		switch n := node.(type) {
		case *promqlparser.BinaryExpr:
			// No vector matching means one operand is not vector. Skip it.
			if n.VectorMatching == nil {
				return nil
			}
			if !n.VectorMatching.On && len(n.VectorMatching.MatchingLabels) == 0 {
				isShardable = false
				return stop
			}
		}
		return nil
	})
	if !isShardable {
		// Mark as not shardable.
		return querysharding.QueryAnalysis{}, nil
	}
	return analysis, nil
}
