package queryrange

import (
	"context"
)

// StepAlignMiddleware aligns the start and end of request to the step to
// improved the cacheability of the query results.
func StepAlignMiddleware(maxStepAlignmentMs int64) MiddlewareFunc {
	return func(next Handler) Handler {
		return stepAlign{
			next:               next,
			maxStepAlignmentMs: maxStepAlignmentMs,
		}
	}
}

type stepAlign struct {
	next               Handler
	maxStepAlignmentMs int64
}

func (s stepAlign) Do(ctx context.Context, r Request) (Response, error) {
	if s.maxStepAlignmentMs > 0 && r.GetStep() > s.maxStepAlignmentMs {
		return s.next.Do(ctx, r)
	}
	start := (r.GetStart() / r.GetStep()) * r.GetStep()
	end := (r.GetEnd() / r.GetStep()) * r.GetStep()
	return s.next.Do(ctx, r.WithStartEnd(start, end))
}
