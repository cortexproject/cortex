package instantquery

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type limitsMiddleware struct {
	tripperware.Limits
	next tripperware.Handler

	lookbackDelta time.Duration
}

// NewLimitsMiddleware creates a new Middleware that enforces query limits.
func NewLimitsMiddleware(l tripperware.Limits, lookbackDelta time.Duration) tripperware.Middleware {
	return tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
		return limitsMiddleware{
			next:   next,
			Limits: l,

			lookbackDelta: lookbackDelta,
		}
	})
}

func (l limitsMiddleware) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	log, ctx := spanlogger.New(ctx, "limits")
	defer log.Finish()

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	// Enforce the max query length.
	if maxQueryLength := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxQueryLength); maxQueryLength > 0 {
		expr, err := parser.ParseExpr(r.GetQuery())
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}

		// Enforce query length across all selectors in the query.
		min, max := promql.FindMinMaxTime(&parser.EvalStmt{Expr: expr, Start: util.TimeFromMillis(0), End: util.TimeFromMillis(0), LookbackDelta: l.lookbackDelta})
		diff := util.TimeFromMillis(max).Sub(util.TimeFromMillis(min))
		if diff > maxQueryLength {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, validation.ErrQueryTooLong, diff, maxQueryLength)
		}
	}

	return l.next.Do(ctx, r)
}
