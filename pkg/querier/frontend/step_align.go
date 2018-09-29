package frontend

import (
	"context"
)

var stepAlignMiddleware queryRangeMiddlewareFunc = func(next queryRangeHandler) queryRangeHandler {
	return stepAlign{
		next: next,
	}
}

type stepAlign struct {
	next queryRangeHandler
}

func (s stepAlign) Do(ctx context.Context, r *queryRangeRequest) (*apiResponse, error) {
	r.start = (r.start / r.step) * r.step
	r.end = (r.end / r.step) * r.step
	return s.next.Do(ctx, r)
}
