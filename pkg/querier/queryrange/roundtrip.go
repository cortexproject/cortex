// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Mostly lifted from prometheus/web/api/v1/api.go.

package queryrange

import (
	"context"
	"flag"
	"net/http"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/go-kit/kit/log"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

type Config struct {
	SplitQueriesByDay    bool `yaml:"split_queries_by_day"`
	AlignQueriesWithStep bool `yaml:"align_queries_with_step"`
	ResultsCacheConfig   `yaml:"results_cache"`
	CacheResults         bool `yaml:"cache_results"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.SplitQueriesByDay, "querier.split-queries-by-day", false, "Split queries by day and execute in parallel.")
	f.BoolVar(&cfg.AlignQueriesWithStep, "querier.align-querier-with-step", false, "Mutate incoming queries to align their start and end with their step.")
	f.BoolVar(&cfg.CacheResults, "querier.cache-results", false, "Cache query results.")
	cfg.ResultsCacheConfig.RegisterFlags(f)
}

// Limits allows us to specify per-tenant runtime limits on the behaviour of
// the query handling code.
type Limits interface {
	MaxQueryLength(string) time.Duration
	MaxQueryParallelism(string) int
}

// HandlerFunc is like http.HandlerFunc, but for Handler.
type HandlerFunc func(context.Context, *Request) (*APIResponse, error)

// Do implements Handler.
func (q HandlerFunc) Do(ctx context.Context, req *Request) (*APIResponse, error) {
	return q(ctx, req)
}

// Handler is like http.Handle, but specifically for Prometheus query_range calls.
type Handler interface {
	Do(context.Context, *Request) (*APIResponse, error)
}

// MiddlewareFunc is like http.HandlerFunc, but for Middleware.
type MiddlewareFunc func(Handler) Handler

// Wrap implements Middleware.
func (q MiddlewareFunc) Wrap(h Handler) Handler {
	return q(h)
}

// Middleware is a higher order Handler.
type Middleware interface {
	Wrap(Handler) Handler
}

// MergeMiddlewares produces a middleware that applies multiple middleware in turn;
// ie Merge(f,g,h).Wrap(handler) == f.Wrap(g.Wrap(h.Wrap(handler)))
func MergeMiddlewares(middleware ...Middleware) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		for i := len(middleware) - 1; i >= 0; i-- {
			next = middleware[i].Wrap(next)
		}
		return next
	})
}

type roundTripper struct {
	next    http.RoundTripper
	handler Handler
	limits  Limits
}

// NewTripperware returns a Tripperware configured with middlewares to align, split and cache requests.
func NewTripperware(cfg Config, log log.Logger, limits Limits) (frontend.Tripperware, error) {
	var queryRangeMiddleware []Middleware
	if cfg.AlignQueriesWithStep {
		queryRangeMiddleware = append(queryRangeMiddleware, InstrumentMiddleware("step_align"), StepAlignMiddleware)
	}
	if cfg.SplitQueriesByDay {
		queryRangeMiddleware = append(queryRangeMiddleware, InstrumentMiddleware("split_by_day"), SplitByDayMiddleware(limits))
	}
	if cfg.CacheResults {
		queryCacheMiddleware, err := NewResultsCacheMiddleware(log, cfg.ResultsCacheConfig, limits)
		if err != nil {
			return nil, err
		}
		queryRangeMiddleware = append(queryRangeMiddleware, InstrumentMiddleware("results_cache"), queryCacheMiddleware)
	}

	return frontend.Tripperware(func(next http.RoundTripper) http.RoundTripper {
		// Finally, if the user selected any query range middleware, stitch it in.
		if len(queryRangeMiddleware) > 0 {
			return NewRoundTripper(next, MergeMiddlewares(queryRangeMiddleware...).Wrap(&ToRoundTripperMiddleware{Next: next}), limits)
		}
		return next
	}), nil
}

// NewRoundTripper wraps a QueryRange Handler and allows it to send requests
// to a http.Roundtripper.
func NewRoundTripper(next http.RoundTripper, handler Handler, limits Limits) http.RoundTripper {
	return roundTripper{
		next:    next,
		handler: handler,
		limits:  limits,
	}
}

func (q roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(r.URL.Path, "/query_range") {
		return q.next.RoundTrip(r)
	}

	request, err := parseRequest(r)
	if err != nil {
		return nil, err
	}

	userid, err := user.ExtractOrgID(r.Context())
	if err != nil {
		return nil, err
	}

	maxQueryLen := q.limits.MaxQueryLength(userid)
	queryLen := timestamp.Time(request.End).Sub(timestamp.Time(request.Start))
	if maxQueryLen != 0 && queryLen > maxQueryLen {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, validation.ErrQueryTooLong, queryLen, maxQueryLen)
	}

	response, err := q.handler.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return response.toHTTPResponse(r.Context())
}

// ToRoundTripperMiddleware not quite sure what this does.
type ToRoundTripperMiddleware struct {
	Next http.RoundTripper
}

// Do implements Handler.
func (q ToRoundTripperMiddleware) Do(ctx context.Context, r *Request) (*APIResponse, error) {
	request, err := r.toHTTPRequest(ctx)
	if err != nil {
		return nil, err
	}

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, request); err != nil {
		return nil, err
	}

	response, err := q.Next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()

	return parseResponse(ctx, response)
}
