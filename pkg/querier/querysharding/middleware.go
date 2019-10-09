package querysharding

import (
	"context"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/prometheus/prometheus/promql"
	"time"
)

const (
	downStreamErrType = "downstream error"
	parseErrType      = "parse error"
)

func QueryShardMiddleware() queryrange.Middleware {
	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return &queryShard{
			next: next,
			mapper: astmapper.NewMultiMapper(
				astmapper.NewShardSummer(astmapper.DEFAULT_SHARDS, astmapper.VectorSquasher),
				astmapper.MapperFunc(astmapper.ShallowEmbedSelectors),
			),
		}
	})
}

type queryShard struct {
	next   queryrange.Handler
	engine *promql.Engine
	mapper astmapper.ASTMapper
}

func (qs *queryShard) Do(ctx context.Context, r *queryrange.Request) (*queryrange.APIResponse, error) {
	queryable := &DownstreamQueryable{r, qs.next}

	qry, err := qs.engine.NewRangeQuery(
		queryable,
		r.Query,
		time.Unix(r.Start, 0), time.Unix(r.End, 0),
		time.Duration(r.Step)*time.Second,
	)

	if err != nil {
		return &queryrange.APIResponse{
			Status:    queryrange.StatusFailure,
			ErrorType: parseErrType,
			Error:     err.Error(),
		}, nil
	}

	res := qry.Exec(ctx)

	if res.Err != nil {
		return &queryrange.APIResponse{
			Status:    queryrange.StatusFailure,
			ErrorType: downStreamErrType,
			Error:     res.Err.Error(),
		}, nil
	}

	if extracted, err := FromValue(res.Value); err != nil {
		return &queryrange.APIResponse{
			Status:    queryrange.StatusFailure,
			ErrorType: downStreamErrType,
			Error:     err.Error(),
		}, nil

	} else {
		return &queryrange.APIResponse{
			Status: queryrange.StatusSuccess,
			Data: queryrange.Response{
				ResultType: string(res.Value.Type()),
				Result:     extracted,
			},
		}, nil
	}
}
