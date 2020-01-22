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
func NewQueryShardMiddleware(
	logger log.Logger,
	engine *promql.Engine,
	confs ShardingConfigs,
	codec Codec,
	minShardingLookback time.Duration,
) Middleware {
	passthrough := MiddlewareFunc(func(next Handler) Handler {
		return next
	})

	noshards := !confs.hasShards()

	if noshards {
		level.Warn(logger).Log(
			"middleware", "QueryShard",
			"msg", "no configuration with shard found",
			"confs", fmt.Sprintf("%+v", confs),
		)
		return passthrough
	}

	getConf := func(r Request) (chunk.PeriodConfig, error) {
		conf, err := confs.ValidRange(r.GetStart(), r.GetEnd())

		// query exists across multiple sharding configs
		if err != nil {
			return conf, err
		}

		// query doesn't have shard factor, so don't try to do AST mapping.
		if conf.RowShards < 2 {
			return conf, errors.Errorf("shard factor not high enough: [%d]", conf.RowShards)
		}

		return conf, nil
	}

	mapperware := MiddlewareFunc(func(next Handler) Handler {
		return &astMapperware{
			getConf: getConf,
			logger:  log.With(logger, "middleware", "QueryShard.astMapperware"),
			next:    next,
		}
	})

	shardingware := MiddlewareFunc(func(next Handler) Handler {
		return &queryShard{
			getConf: getConf,
			next:    next,
			engine:  engine,
		}
	})

	return MiddlewareFunc(func(next Handler) Handler {
		return &shardSplitter{
			codec:               codec,
			MinShardingLookback: minShardingLookback,
			shardingware: MergeMiddlewares(
				InstrumentMiddleware("shardingware"),
				mapperware,
				shardingware,
			).Wrap(next),
			now:  time.Now,
			next: InstrumentMiddleware("sharding-bypass").Wrap(next),
		}
	})

}

type astMapperware struct {
	getConf func(Request) (chunk.PeriodConfig, error)
	logger  log.Logger
	next    Handler
}

func (ast *astMapperware) Do(ctx context.Context, r Request) (Response, error) {
	conf, err := ast.getConf(r)
	// cannot shard with this timerange
	if err != nil {
		level.Warn(ast.logger).Log("err", err.Error())
		return ast.next.Do(ctx, r)
	}

	shardSummer, err := astmapper.NewShardSummer(int(conf.RowShards), astmapper.VectorSquasher)
	if err != nil {
		return nil, err
	}

	subtreeFolder, err := astmapper.NewSubtreeFolder(astmapper.JSONCodec)
	if err != nil {
		return nil, err
	}

	strQuery := r.GetQuery()
	mappedQuery, err := mapQuery(
		astmapper.NewMultiMapper(
			shardSummer,
			subtreeFolder,
		),
		strQuery,
	)

	if err != nil {
		return nil, err
	}

	strMappedQuery := mappedQuery.String()
	level.Debug(ast.logger).Log("msg", "mapped query", "original", strQuery, "mapped", strMappedQuery)

	return ast.next.Do(ctx, r.WithQuery(strMappedQuery))

}

type queryShard struct {
	getConf func(Request) (chunk.PeriodConfig, error)
	next    Handler
	engine  *promql.Engine
}

func (qs *queryShard) Do(ctx context.Context, r Request) (Response, error) {
	// since there's no available sharding configuration for this time range,
	// no astmapping has been performed, so skip this middleware.
	if _, err := qs.getConf(r); err != nil {
		return qs.next.Do(ctx, r)
	}

	queryable := lazyquery.NewLazyQueryable(&DownstreamQueryable{r, qs.next})

	qry, err := qs.engine.NewRangeQuery(
		queryable,
		r.GetQuery(),
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

// shardSplitter middleware will only shard appropriate requests that do not extend past the MinShardingLookback interval.
// This is used to send nonsharded requests to the ingesters in order to not overload them.
type shardSplitter struct {
	codec               Codec
	MinShardingLookback time.Duration    // delimiter for splitting sharded vs non-sharded queries
	shardingware        Handler          // handler for sharded queries
	next                Handler          // handler for non-sharded queries
	now                 func() time.Time // injectable time.Now
}

func (splitter *shardSplitter) Do(ctx context.Context, r Request) (Response, error) {
	cutoff := splitter.now().Add(-splitter.MinShardingLookback)
	sharded, nonsharded := partitionRequest(r, cutoff)

	return splitter.parallel(ctx, sharded, nonsharded)

}

func (splitter *shardSplitter) parallel(ctx context.Context, sharded, nonsharded Request) (Response, error) {
	if sharded == nil {
		return splitter.next.Do(ctx, nonsharded)
	}

	if nonsharded == nil {
		return splitter.shardingware.Do(ctx, sharded)
	}

	nonshardCh := make(chan Response, 1)
	shardCh := make(chan Response, 1)
	errCh := make(chan error, 2)

	go func() {
		res, err := splitter.next.Do(ctx, nonsharded)
		if err != nil {
			errCh <- err
			return
		}
		nonshardCh <- res

	}()

	go func() {
		res, err := splitter.shardingware.Do(ctx, sharded)
		if err != nil {
			errCh <- err
			return
		}
		shardCh <- res
	}()

	resps := make([]Response, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case r := <-nonshardCh:
			resps = append(resps, r)
		case r := <-shardCh:
			resps = append(resps, r)
		case err := <-errCh:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	}

	return splitter.codec.MergeResponse(resps...)
}

// partitionQuery splits a request into potentially multiple requests, one including the request's time range
// [0,t). The other will include [t,inf)
func partitionRequest(r Request, t time.Time) (before Request, after Request) {
	boundary := TimeToMillis(t)
	if r.GetStart() >= boundary {
		return nil, r
	}

	if r.GetEnd() < boundary {
		return r, nil
	}

	return r.WithStartEnd(r.GetStart(), boundary), r.WithStartEnd(boundary, r.GetEnd())
}

// TimeFromMillis is a helper to turn milliseconds -> time.Time
func TimeFromMillis(ms int64) time.Time {
	secs := ms / 1000
	rem := ms - (secs * 1000)
	return time.Unix(secs, rem*nanosecondsInMillisecond)
}

func TimeToMillis(t time.Time) int64 {
	return t.UnixNano() / nanosecondsInMillisecond
}
