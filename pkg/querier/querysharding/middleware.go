package querysharding

import (
	"context"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/cortexproject/cortex/pkg/querier/lazyquery"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
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

// ShardingConfig will eventually support ranges like chunk.PeriodConfig for meshing togethe multiple queryShard(s).
// This will enable compatibility for deployments which change their shard factors over time.
type ShardingConfig struct {
	From   chunk.DayTime `yaml:"from"`
	Shards int           `yaml:"shards"`
}

// ShardingConfigs is a slice of shards
type ShardingConfigs []ShardingConfig

// ValidRange extracts a non-overlapping sharding configuration from a list of configs and a time range.
func (confs ShardingConfigs) ValidRange(start, end int64) (ShardingConfig, error) {
	for i, conf := range confs {
		if start < int64(conf.From.Time) {
			// the query starts before this config's range
			return ShardingConfig{}, errInvalidShardingRange
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

	return ShardingConfig{}, errInvalidShardingRange

}

func mapQuery(mapper astmapper.ASTMapper, query string) (promql.Node, error) {
	expr, err := promql.ParseExpr(query)
	if err != nil {
		return nil, err
	}
	return mapper.Map(expr)
}

// QueryShardMiddleware creates a middleware which downstreams queries after AST mapping and query encoding.
func QueryShardMiddleware(engine *promql.Engine, confs ShardingConfigs) queryrange.Middleware {
	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return &queryShard{
			confs:  confs,
			next:   next,
			engine: engine,
		}
	})
}

type queryShard struct {
	confs  ShardingConfigs
	next   queryrange.Handler
	engine *promql.Engine
}

func (qs *queryShard) Do(ctx context.Context, r *queryrange.Request) (*queryrange.APIResponse, error) {
	queryable := lazyquery.NewLazyQueryable(&DownstreamQueryable{r, qs.next})

	conf, err := qs.confs.ValidRange(r.Start, r.End)
	// query exists across multiple sharding configs, so don't try to do AST mapping.
	if err != nil {
		return qs.next.Do(ctx, r)
	}

	mappedQuery, err := mapQuery(
		astmapper.NewMultiMapper(
			astmapper.NewShardSummer(conf.Shards, astmapper.VectorSquasher),
			astmapper.ShallowEmbedSelectors,
		),
		r.Query,
	)

	if err != nil {
		return nil, err
	}

	qry, err := qs.engine.NewRangeQuery(
		queryable,
		mappedQuery.String(),
		TimeFromMillis(r.Start),
		TimeFromMillis(r.End),
		time.Duration(r.Step)*time.Millisecond,
	)

	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)

	// TODO(owen-d): Unclear on whether error belongs in APIResponse struct or as 2nd value in return tuple
	if res.Err != nil {
		return &queryrange.APIResponse{
			Status:    queryrange.StatusFailure,
			ErrorType: downStreamErrType,
			Error:     res.Err.Error(),
		}, nil
	}

	extracted, err := FromValue(res.Value)
	if err != nil {
		return &queryrange.APIResponse{
			Status:    queryrange.StatusFailure,
			ErrorType: downStreamErrType,
			Error:     err.Error(),
		}, nil

	}
	return &queryrange.APIResponse{
		Status: queryrange.StatusSuccess,
		Data: queryrange.Response{
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
