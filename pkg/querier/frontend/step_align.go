package frontend

import (
	"context"
)

var stepAlignMiddleware = queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
	return instrument("step_align").Wrap(stepAlign{
		next: next,
	})
})

type stepAlign struct {
	next queryRangeHandler
}

func (s stepAlign) Do(ctx context.Context, r *QueryRangeRequest) (*APIResponse, error) {
	r.Start = (r.Start / r.Step) * r.Step
	r.End = (r.End / r.Step) * r.Step
	return s.next.Do(ctx, r)
}
