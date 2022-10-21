package queryrange

import (
	"context"
	"net/http"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/timestamp"
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
}

// NewLimitsMiddleware creates a new Middleware that enforces query limits.
func NewLimitsMiddleware(l tripperware.Limits) tripperware.Middleware {
	return tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
		return limitsMiddleware{
			next:   next,
			Limits: l,
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

	// Clamp the time range based on the max query lookback.

	if maxQueryLookback := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxQueryLookback); maxQueryLookback > 0 {
		minStartTime := util.TimeToMillis(time.Now().Add(-maxQueryLookback))

		if r.GetEnd() < minStartTime {
			// The request is fully outside the allowed range, so we can return an
			// empty response.
			level.Debug(log).Log(
				"msg", "skipping the execution of the query because its time range is before the 'max query lookback' setting",
				"reqStart", util.FormatTimeMillis(r.GetStart()),
				"redEnd", util.FormatTimeMillis(r.GetEnd()),
				"maxQueryLookback", maxQueryLookback)

			return NewEmptyPrometheusResponse(), nil
		}

		if r.GetStart() < minStartTime {
			// Replace the start time in the request.
			level.Debug(log).Log(
				"msg", "the start time of the query has been manipulated because of the 'max query lookback' setting",
				"original", util.FormatTimeMillis(r.GetStart()),
				"updated", util.FormatTimeMillis(minStartTime))

			r = r.WithStartEnd(minStartTime, r.GetEnd())
		}
	}

	// Enforce the max query length.
	if maxQueryLength := validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, l.MaxQueryLength); maxQueryLength > 0 {
		queryLen := timestamp.Time(r.GetEnd()).Sub(timestamp.Time(r.GetStart()))
		if queryLen > maxQueryLength {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, validation.ErrQueryTooLong, queryLen, maxQueryLength)
		}
	}

	return l.next.Do(ctx, r)
}
