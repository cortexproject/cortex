package queryrange

import (
	"context"
	"time"
)

const millisecondPerDay = int64(24 * time.Hour / time.Millisecond)

// SplitByDayMiddleware creates a new Middleware that splits requests by day.
func SplitByDayMiddleware(limits Limits, merger Merger) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return splitByDay{
			next:   next,
			limits: limits,
			merger: merger,
		}
	})
}

type splitByDay struct {
	next   Handler
	limits Limits
	merger Merger
}

func (s splitByDay) Do(ctx context.Context, r Request) (Response, error) {
	// First we're going to build new requests, one for each day, taking care
	// to line up the boundaries with step.
	reqs := splitQuery(r)

	reqResps, err := doRequests(ctx, s.next, reqs, s.limits)
	if err != nil {
		return nil, err
	}

	resps := make([]Response, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.resp)
	}

	response, err := s.merger.MergeResponse(resps...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func splitQuery(r Request) []Request {
	var reqs []Request
	for start := r.GetStart(); start < r.GetEnd(); start = nextDayBoundary(start, r.GetStep()) + r.GetStep() {
		end := nextDayBoundary(start, r.GetStep())
		if end+r.GetStep() >= r.GetEnd() {
			end = r.GetEnd()
		}

		reqs = append(reqs, r.WithStartEnd(start, end))
	}
	return reqs
}

// Round up to the step before the next day boundary.
func nextDayBoundary(t, step int64) int64 {
	startOfNextDay := ((t / millisecondPerDay) + 1) * millisecondPerDay
	// ensure that target is a multiple of steps away from the start time
	target := startOfNextDay - ((startOfNextDay - t) % step)
	if target == startOfNextDay {
		target -= step
	}
	return target
}
