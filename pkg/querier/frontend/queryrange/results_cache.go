package queryrange

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/user"
)

type ResultsCacheConfig struct {
	cacheConfig       cache.Config
	MaxCacheFreshness time.Duration
}

func (cfg *ResultsCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.cacheConfig.RegisterFlagsWithPrefix("", "", f)
	f.DurationVar(&cfg.MaxCacheFreshness, "frontend.max-cache-freshness", 1*time.Minute, "Most recent allowed cacheable result, to prevent caching very recent results that might still be in flux.")
}

type resultsCacheMiddleware struct {
	logger            log.Logger
	maxCacheFreshness time.Duration
	next              Handler
	cache             cache.Cache
	limits            Limits
}

// NewResultsCacheMiddlewareFromConfig creates results cache middleware from config.
func NewResultsCacheMiddlewareFromConfig(logger log.Logger, cfg ResultsCacheConfig, limits Limits) (Middleware, error) {
	c, err := cache.New(cfg.cacheConfig)
	if err != nil {
		return nil, err
	}

	return NewResultsCacheMiddleware(logger, cache.NewSnappy(c), cfg.MaxCacheFreshness, limits)
}

// NewResultsCacheMiddleware creates results cache middleware using given cache client.
// NOTE: It is recommend to wrap it with cache.NewSnappy(..).
func NewResultsCacheMiddleware(logger log.Logger, cacheClient cache.Cache, maxCacheFreshness time.Duration, limits Limits) (Middleware, error) {
	return MiddlewareFunc(func(next Handler) Handler {
		return &resultsCacheMiddleware{
			logger:            logger,
			maxCacheFreshness: maxCacheFreshness,
			next:              next,
			cache:             cacheClient,
			limits:            limits,
		}
	}), nil
}

func (s resultsCacheMiddleware) Do(ctx context.Context, r *Request) (*APIResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	var (
		day      = r.Start / millisecondPerDay
		key      = fmt.Sprintf("%s:%s:%d:%d", userID, r.Query, r.Step, day)
		extents  []Extent
		response *APIResponse
	)

	maxCacheTime := int64(model.Now().Add(-s.maxCacheFreshness))
	if r.Start > maxCacheTime {
		return s.next.Do(ctx, r)
	}

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

func (s resultsCacheMiddleware) handleMiss(ctx context.Context, r *Request) (*APIResponse, []Extent, error) {
	response, err := s.next.Do(ctx, r)
	if err != nil {
		return nil, nil, err
	}

	extents := []Extent{
		{
			Start:    r.Start,
			End:      r.End,
			Response: response,
		},
	}
	return response, extents, nil
}

func (s resultsCacheMiddleware) handleHit(ctx context.Context, r *Request, extents []Extent) (*APIResponse, []Extent, error) {
	var (
		reqResps []requestResponse
		err      error
	)

	requests, responses := partition(r, extents)
	if len(requests) == 0 {
		response, err := MergeAPIResponses(responses)
		// No downstream requests so no need to write back to the cache.
		return response, nil, err
	}

	reqResps, err = doRequests(ctx, s.next, requests, s.limits)
	if err != nil {
		return nil, nil, err
	}

	for _, reqResp := range reqResps {
		responses = append(responses, reqResp.resp)
		extents = append(extents, Extent{
			Start:    reqResp.req.Start,
			End:      reqResp.req.End,
			Response: reqResp.resp,
		})
	}
	sort.Slice(extents, func(i, j int) bool {
		return extents[i].Start < extents[j].Start
	})

	// Merge any extents - they're guaranteed not to overlap.
	accumulator, mergedExtents := extents[0], make([]Extent, 0, len(extents))
	for i := 1; i < len(extents); i++ {
		if accumulator.End+r.Step < extents[i].Start {
			mergedExtents = append(mergedExtents, accumulator)
			accumulator = extents[i]
			continue
		}

		accumulator.End = extents[i].End
		accumulator.Response, err = MergeAPIResponses([]*APIResponse{accumulator.Response, extents[i].Response})
		if err != nil {
			return nil, nil, err
		}

	}
	mergedExtents = append(mergedExtents, accumulator)

	response, err := MergeAPIResponses(responses)
	return response, mergedExtents, err
}

// partition calculates the required requests to satisfy req given the cached data.
func partition(req *Request, extents []Extent) ([]*Request, []*APIResponse) {
	var requests []*Request
	var cachedResponses []*APIResponse
	start := req.Start

	for _, extent := range extents {
		// If there is no overlap, ignore this extent.
		if extent.End < start || extent.Start > req.End {
			continue
		}

		// If there is a bit missing at the front, make a request for that.
		if start < extent.Start {
			r := req.Copy()
			r.Start = start
			r.End = extent.Start
			requests = append(requests, &r)
		}

		// extract the overlap from the cached extent.
		cachedResponses = append(cachedResponses, extract(start, req.End, extent))
		start = extent.End
	}

	if start < req.End {
		r := req.Copy()
		r.Start = start
		r.End = req.End
		requests = append(requests, &r)
	}

	return requests, cachedResponses
}

func (s resultsCacheMiddleware) filterRecentExtents(req *Request, extents []Extent) []Extent {
	maxCacheTime := (int64(model.Now().Add(-s.maxCacheFreshness)) / req.Step) * req.Step
	for i := range extents {
		// Never cache data for the latest freshness period.
		if extents[i].End > maxCacheTime {
			extents[i].End = maxCacheTime
			extents[i].Response = extract(extents[i].Start, maxCacheTime, extents[i])
		}
	}
	return extents
}

func (s resultsCacheMiddleware) get(ctx context.Context, key string) ([]Extent, bool) {
	found, bufs, _ := s.cache.Fetch(ctx, []string{cache.HashKey(key)})
	if len(found) != 1 {
		return nil, false
	}

	var resp CachedResponse
	sp, _ := opentracing.StartSpanFromContext(ctx, "unmarshal-extent")
	defer sp.Finish()

	sp.LogFields(otlog.Int("bytes", len(bufs[0])))

	if err := proto.Unmarshal(bufs[0], &resp); err != nil {
		level.Error(s.logger).Log("msg", "error unmarshalling cached value", "err", err)
		sp.LogFields(otlog.Error(err))
		return nil, false
	}

	if resp.Key != key {
		return nil, false
	}

	return resp.Extents, true
}

func (s resultsCacheMiddleware) put(ctx context.Context, key string, extents []Extent) {
	buf, err := proto.Marshal(&CachedResponse{
		Key:     key,
		Extents: extents,
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "error marshalling cached value", "err", err)
		return
	}

	s.cache.Store(ctx, []string{cache.HashKey(key)}, [][]byte{buf})
}
