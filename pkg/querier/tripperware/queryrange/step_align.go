package queryrange

import (
	"context"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

// StepAlignMiddleware aligns the start and end of request to the step to
// improved the cacheability of the query results.
var StepAlignMiddleware = tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
	return stepAlign{
		next: next,
	}
})

type stepAlign struct {
	next tripperware.Handler
}

func (s stepAlign) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	start := (r.GetStart() / r.GetStep()) * r.GetStep()
	end := (r.GetEnd() / r.GetStep()) * r.GetStep()
	return s.next.Do(ctx, r.WithStartEnd(start, end))
}
