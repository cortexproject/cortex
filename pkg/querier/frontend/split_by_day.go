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
	req  queryRangeRequest
	resp *apiResponse
	err  error
}

func (s splitByDay) Do(ctx context.Context, r *queryRangeRequest) (*apiResponse, error) {
	// First we're going to build new requests, one for each day, taking care
	// to line up the boundaries with step.
	reqs := splitQuery(r)

	reqResps, err := doRequests(ctx, s.next, reqs)
	if err != nil {
		return nil, err
	}

	resps := make([]*apiResponse, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.resp)
	}

	return mergeAPIResponses(resps)
}

func splitQuery(r *queryRangeRequest) []*queryRangeRequest {
	reqs := []*queryRangeRequest{}
	for start := r.start; start < r.end; start = nextDayBoundary(start, r.step) + r.step {
		end := nextDayBoundary(start, r.step)
		if end+r.step >= r.end {
			end = r.end
		}

		reqs = append(reqs, &queryRangeRequest{
			path:  r.path,
			start: start,
			end:   end,
			step:  r.step,
			query: r.query,
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
	req  *queryRangeRequest
	resp *apiResponse
}

func doRequests(ctx context.Context, downstream queryRangeHandler, reqs []*queryRangeRequest) ([]requestResponse, error) {
	// If one of the requests fail, we want to be able to cancel the rest of them.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	respChan, errChan := make(chan requestResponse), make(chan error)
	for _, req := range reqs {
		go func(req *queryRangeRequest) {
			resp, err := downstream.Do(ctx, req)
			if err != nil {
				errChan <- err
			} else {
				respChan <- requestResponse{req, resp}
			}
		}(req)
	}

	resps := make([]requestResponse, 0, len(reqs))
	var firstErr error
	for range reqs {
		select {
		case resp := <-respChan:
			resps = append(resps, resp)
		case err := <-errChan:
			if firstErr == nil {
				cancel()
				firstErr = err
			}
		}
	}

	return resps, firstErr
}
