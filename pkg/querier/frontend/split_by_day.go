package frontend

import (
	"context"
	"time"
)

const millisecondPerDay = int64(24 * time.Hour / time.Millisecond)

var splitByDayMiddleware queryRangeMiddlewareFunc = func(next queryRangeHandler) queryRangeHandler {
	return splitByDay{
		next: next,
	}
}

type splitByDay struct {
	next queryRangeHandler
}

type response struct {
	req  QueryRangeRequest
	resp *APIResponse
	err  error
}

func (s splitByDay) Do(ctx context.Context, r *QueryRangeRequest) (*APIResponse, error) {
	// First we're going to build new requests, one for each day, taking care
	// to line up the boundaries with step.
	reqs := splitQuery(r)

	reqResps, err := doRequests(ctx, s.next, reqs)
	if err != nil {
		return nil, err
	}

	resps := make([]*APIResponse, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.resp)
	}

	return mergeAPIResponses(resps)
}

func splitQuery(r *QueryRangeRequest) []*QueryRangeRequest {
	reqs := []*QueryRangeRequest{}
	for start := r.Start; start < r.End; start = nextDayBoundary(start, r.Step) + r.Step {
		end := nextDayBoundary(start, r.Step)
		if end+r.Step >= r.End {
			end = r.End
		}

		reqs = append(reqs, &QueryRangeRequest{
			Path:  r.Path,
			Start: start,
			End:   end,
			Step:  r.Step,
			Query: r.Query,
		})
	}
	return reqs
}

// Round up to the step before the next day boundary.
func nextDayBoundary(t, step int64) int64 {
	offsetToDayBoundary := step - (t % millisecondPerDay % step)
	t = ((t / millisecondPerDay) + 1) * millisecondPerDay
	return t - offsetToDayBoundary
}

type requestResponse struct {
	req  *QueryRangeRequest
	resp *APIResponse
}
