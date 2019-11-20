package queryrange

import (
	"context"
	fmt "fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/cortexproject/cortex/pkg/querier/lazyquery"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

const (
	downStreamErrType = "downstream error"
)

var (
	nanosecondsInMillisecond = int64(time.Millisecond / time.Nanosecond)

	errInvalidShardingRange = errors.New("Query does not fit in a single sharding configuration")
)

// ShardingConfigs is a slice of chunk shard configs
type ShardingConfigs []chunk.PeriodConfig

// ValidRange extracts a non-overlapping sharding configuration from a list of configs and a time range.
func (confs ShardingConfigs) ValidRange(start, end int64) (chunk.PeriodConfig, error) {
	for i, conf := range confs {
		if start < int64(conf.From.Time) {
			// the query starts before this config's range
			return chunk.PeriodConfig{}, errInvalidShardingRange
		} else if i == len(confs)-1 {
			// the last configuration has no upper bound
			return conf, nil
		} else if end < int64(confs[i+1].From.Time) {
			// The request is entirely scoped into this shard config
			return conf, nil
		} else {
			continue
		}
	}

	return chunk.PeriodConfig{}, errInvalidShardingRange
}

func (confs ShardingConfigs) hasShards() bool {
	for _, conf := range confs {
		if conf.RowShards > 0 {
			return true
		}
	}
	return false
}

func mapQuery(mapper astmapper.ASTMapper, query string) (promql.Node, error) {
	expr, err := promql.ParseExpr(query)
	if err != nil {
		return nil, err
	}
	return mapper.Map(expr)
}

// NewQueryShardMiddleware creates a middleware which downstreams queries after AST mapping and query encoding.
func NewQueryShardMiddleware(logger log.Logger, engine *promql.Engine, confs ShardingConfigs) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		if !confs.hasShards() {
			level.Warn(logger).Log("msg", "no configuration with shard found", "confs", fmt.Sprintf("%+v", confs))
			return next
		}
		return &queryShard{
			logger: log.With(logger, "middleware", "QueryShard"),
			confs:  confs,
			next:   next,
			engine: engine,
		}
	})
}

type queryShard struct {
	logger log.Logger
	confs  ShardingConfigs
	next   Handler
	engine *promql.Engine
}

func (qs *queryShard) Do(ctx context.Context, r Request) (Response, error) {

	conf, err := qs.confs.ValidRange(r.GetStart(), r.GetEnd())
	// query exists across multiple sharding configs or doesn't have sharding, so don't try to do AST mapping.
	if err != nil || conf.RowShards < 2 {
		return qs.next.Do(ctx, r)
	}

	queryable := lazyquery.NewLazyQueryable(&DownstreamQueryable{r, qs.next})

	shardSummer, err := astmapper.NewShardSummer(int(conf.RowShards), astmapper.VectorSquasher)
	if err != nil {
		return nil, err
	}

	strQuery := r.GetQuery()
	mappedQuery, err := mapQuery(
		astmapper.NewMultiMapper(
			shardSummer,
			astmapper.ShallowEmbedSelectors,
		),
		strQuery,
	)

	if err != nil {
		return nil, err
	}

	strMappedQuery := mappedQuery.String()
	if strMappedQuery == strQuery {
		level.Debug(qs.logger).Log("msg", "unaltered query", "query", strQuery)
	} else {
		level.Debug(qs.logger).Log("msg", "mapped query", "original", strQuery, "mapped", strMappedQuery)
	}

	qry, err := qs.engine.NewRangeQuery(
		queryable,
		strMappedQuery,
		TimeFromMillis(r.GetStart()),
		TimeFromMillis(r.GetEnd()),
		time.Duration(r.GetStep())*time.Millisecond,
	)

	if err != nil {
		return nil, err
	}
	res := qry.Exec(ctx)
	extracted, err := FromResult(res)
	if err != nil {
		return nil, err

	}
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
	}, nil
}

// TimeFromMillis is a helper to turn milliseconds -> time.Time
func TimeFromMillis(ms int64) time.Time {
	secs := ms / 1000
	rem := ms - (secs * 1000)
	return time.Unix(secs, rem*nanosecondsInMillisecond)
}
