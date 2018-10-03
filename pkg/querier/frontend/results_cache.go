package frontend

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/user"
)

type resultsCacheConfig struct {
	cacheConfig       cache.Config
	MaxCacheFreshness time.Duration
}

func (cfg *resultsCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.cacheConfig.RegisterFlags(f)
	f.DurationVar(&cfg.MaxCacheFreshness, "frontend.max-cache-freshness", 1*time.Minute, "Most recent allowed cacheable result, to prevent caching very recent results that might still be in flux.")
}

type resultsCache struct {
	cfg   resultsCacheConfig
	next  queryRangeHandler
	cache cache.Cache
}

func newResultsCacheMiddleware(cfg resultsCacheConfig) (queryRangeMiddleware, error) {
	c, err := cache.New(cfg.cacheConfig)
	if err != nil {
		return nil, err
	}

	return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
		return &resultsCache{
			cfg:   cfg,
			next:  next,
			cache: cache.NewSnappy(c),
		}
	}), nil
}

type cachedResponse struct {
	Key string `json:"key"`

	// List of cached responses; non-overlapping and in order.
	Extents []extent `json:"extents"`
}

type extent struct {
	Start    int64        `json:"start"`
	End      int64        `json:"end"`
	Response *apiResponse `json:"response"`
}

func (s resultsCache) Do(ctx context.Context, r *queryRangeRequest) (*apiResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	var (
		day      = r.start / millisecondPerDay
		key      = fmt.Sprintf("%s:%s:%d:%d", userID, r.query, r.step, day)
		extents  []extent
		response *apiResponse
	)

	cached, ok := s.get(ctx, key)
	if ok {
		response, extents, err = s.handleHit(ctx, r, cached)
	} else {
		response, extents, err = s.handleMiss(ctx, r)
	}

	if err == nil && len(extents) > 0 {
		extents = s.filterRecentExtents(r, extents)
		s.put(ctx, key, extents)
	}

	return response, err
}

func (s resultsCache) handleMiss(ctx context.Context, r *queryRangeRequest) (*apiResponse, []extent, error) {
	response, err := s.next.Do(ctx, r)
	if err != nil {
		return nil, nil, err
	}

	extents := []extent{
		{
			Start:    r.start,
			End:      r.end,
			Response: response,
		},
	}
	return response, extents, nil
}

func (s resultsCache) handleHit(ctx context.Context, r *queryRangeRequest, extents []extent) (*apiResponse, []extent, error) {
	var (
		reqResps []requestResponse
		err      error
	)

	requests, responses := partition(r, extents)
	if len(requests) == 0 {
		response, err := mergeAPIResponses(responses)
		// No downstream requests so no need to write back to the cache.
		return response, nil, err
	}

	reqResps, err = doRequests(ctx, s.next, requests)
	if err != nil {
		return nil, nil, err
	}

	for _, reqResp := range reqResps {
		responses = append(responses, reqResp.resp)
		extents = append(extents, extent{
			Start:    reqResp.req.start,
			End:      reqResp.req.end,
			Response: reqResp.resp,
		})
	}
	sort.Slice(extents, func(i, j int) bool {
		return extents[i].Start < extents[j].Start
	})

	// Merge any extents - they're guaranteed not to overlap.
	accumulator, mergedExtents := extents[0], make([]extent, 0, len(extents))
	for i := 1; i < len(extents); i++ {
		if accumulator.End+r.step < extents[i].Start {
			mergedExtents = append(mergedExtents, accumulator)
			accumulator = extents[i]
			continue
		}

		accumulator.End = extents[i].End
		accumulator.Response, err = mergeAPIResponses([]*apiResponse{accumulator.Response, extents[i].Response})
		if err != nil {
			return nil, nil, err
		}

	}
	mergedExtents = append(mergedExtents, accumulator)

	response, err := mergeAPIResponses(responses)
	return response, mergedExtents, err
}

// partition calculates the required requests to satisfy req given the cached data.
func partition(req *queryRangeRequest, extents []extent) ([]*queryRangeRequest, []*apiResponse) {
	var requests []*queryRangeRequest
	var cachedResponses []*apiResponse
	start := req.start

	for _, extent := range extents {
		// If there is no overlap, ignore this extent.
		if extent.End < start || extent.Start > req.end {
			continue
		}

		// If there is a bit missing at the front, make a request for that.
		if start < extent.Start {
			r := req.copy()
			r.start = start
			r.end = extent.Start
			requests = append(requests, &r)
		}

		// Extract the overlap from the cached extent.
		cachedResponses = append(cachedResponses, extract(start, req.end, extent))
		start = extent.End
	}

	if start < req.end {
		r := req.copy()
		r.start = start
		r.end = req.end
		requests = append(requests, &r)
	}

	return requests, cachedResponses
}

func (s resultsCache) filterRecentExtents(req *queryRangeRequest, extents []extent) []extent {
	maxCacheTime := (int64(model.Now().Add(-s.cfg.MaxCacheFreshness)) / req.step) * req.step
	for i := range extents {
		// Never cache data for the latest freshness period.
		if extents[i].End > maxCacheTime {
			extents[i].End = maxCacheTime
			extents[i].Response = extract(extents[i].Start, maxCacheTime, extents[i])
		}
	}
	return extents
}

func (s resultsCache) get(ctx context.Context, key string) ([]extent, bool) {
	found, buf, _ := s.cache.Fetch(ctx, []string{cache.HashKey(key)})
	if len(found) != 1 {
		return nil, false
	}

	var resp cachedResponse
	if err := json.Unmarshal(buf[0], &resp); err != nil {
		level.Error(util.Logger).Log("msg", "error unmarshaling cached value", "err", err)
		return nil, false
	}

	if resp.Key != key {
		return nil, false
	}

	return resp.Extents, true
}

func (s resultsCache) put(ctx context.Context, key string, extents []extent) {
	buf, err := json.Marshal(cachedResponse{
		Key:     key,
		Extents: extents,
	})
	if err != nil {
		level.Error(util.Logger).Log("msg", "error marshalling cached value", "err", err)
		return
	}

	s.cache.Store(ctx, []string{cache.HashKey(key)}, [][]byte{buf})
}
