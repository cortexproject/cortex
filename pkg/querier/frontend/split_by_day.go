package frontend

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/common/model"
)

const millisecondPerDay = int64(24 * time.Hour / time.Millisecond)

type splitByDay struct {
	downstream queryRangeMiddleware
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

	// Next, do the requests in parallel.
	// If one of the requests fail, we want to be able to cancel the rest of them.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resps := make(chan *apiResponse)
	errs := make(chan error)
	for _, req := range reqs {
		go func(req *queryRangeRequest) {
			resp, err := s.downstream.Do(ctx, req)
			if err != nil {
				errs <- err
			} else {
				resps <- resp
			}
		}(req)
	}

	// Gather up the responses and errors.
	var responses []*apiResponse
	var firstErr error
	for range reqs {
		select {
		case resp := <-resps:
			responses = append(responses, resp)
		case err := <-errs:
			// Only record the first error, as subsequent errors are cancellations.
			if firstErr == nil {
				firstErr = err
				cancel()
			}
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return mergeAPIResponses(responses)
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

func mergeAPIResponses(responses []*apiResponse) (*apiResponse, error) {
	// Merge the responses.
	sort.Sort(byFirstTime(responses))

	if len(responses) == 0 {
		return &apiResponse{
			Status: statusSuccess,
		}, nil
	}

	switch responses[0].Data.Result.(type) {
	case model.Vector:
		return vectorMerge(responses)
	case model.Matrix:
		return matrixMerge(responses)
	default:
		return nil, fmt.Errorf("unexpected response type")
	}
}

type byFirstTime []*apiResponse

func (a byFirstTime) Len() int           { return len(a) }
func (a byFirstTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFirstTime) Less(i, j int) bool { return minTime(a[i]) < minTime(a[j]) }

func minTime(resp *apiResponse) model.Time {
	switch result := resp.Data.Result.(type) {
	case model.Vector:
		if len(result) == 0 {
			return -1
		}
		return result[0].Timestamp

	case model.Matrix:
		if len(result) == 0 {
			return -1
		}
		if len(result[0].Values) == 0 {
			return -1
		}
		return result[0].Values[0].Timestamp

	default:
		return -1
	}
}

func vectorMerge(resps []*apiResponse) (*apiResponse, error) {
	var output model.Vector
	for _, resp := range resps {
		output = append(output, resp.Data.Result.(model.Vector)...)
	}
	return &apiResponse{
		Status: statusSuccess,
		Data: queryRangeResponse{
			ResultType: model.ValVector,
			Result:     output,
		},
	}, nil
}

func matrixMerge(resps []*apiResponse) (*apiResponse, error) {
	output := map[string]*model.SampleStream{}
	for _, resp := range resps {
		matrix := resp.Data.Result.(model.Matrix)
		for _, stream := range matrix {
			metric := stream.Metric.String()
			existing, ok := output[metric]
			if !ok {
				existing = &model.SampleStream{
					Metric: stream.Metric,
				}
			}
			existing.Values = append(existing.Values, stream.Values...)
			output[metric] = existing
		}
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make(model.Matrix, 0, len(output))
	for _, key := range keys {
		result = append(result, output[key])
	}

	return &apiResponse{
		Status: statusSuccess,
		Data: queryRangeResponse{
			ResultType: model.ValMatrix,
			Result:     result,
		},
	}, nil
}
