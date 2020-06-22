package queryrange

import (
	"context"
)

// StepAlignMiddleware aligns the start and end of request to the step to
// improved the cacheability of the query results.
func StepAlignMiddleware(maxStepAlignment int64) MiddlewareFunc {
	return func(next Handler) Handler {
		return stepAlign{
			next:             next,
			maxStepAlignment: maxStepAlignment,
		}
	}
}

type stepAlign struct {
	next             Handler
	maxStepAlignment int64
}

func (s stepAlign) Do(ctx context.Context, r Request) (Response, error) {
	if s.maxStepAlignment > 0 && r.GetStep() > s.maxStepAlignment {
		return s.next.Do(ctx, r)
	}
	start := (r.GetStart() / r.GetStep()) * r.GetStep()
	end := (r.GetEnd() / r.GetStep()) * r.GetStep()
	return s.next.Do(ctx, r.WithStartEnd(start, end))
}
