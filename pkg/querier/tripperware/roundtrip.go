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

package tripperware

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

const (
	opTypeQuery          = "query"
	opTypeQueryRange     = "query_range"
	opTypeSeries         = "series"
	opTypeRemoteRead     = "remote_read"
	opTypeLabelNames     = "label_names"
	opTypeLabelValues    = "label_values"
	opTypeMetadata       = "metadata"
	opTypeQueryExemplars = "query_exemplars"
)

// HandlerFunc is like http.HandlerFunc, but for Handler.
type HandlerFunc func(context.Context, Request) (Response, error)

// Do implements Handler.
func (q HandlerFunc) Do(ctx context.Context, req Request) (Response, error) {
	return q(ctx, req)
}

// Tripperware is a signature for all http client-side middleware.
type Tripperware func(http.RoundTripper) http.RoundTripper

// RoundTripFunc is to http.RoundTripper what http.HandlerFunc is to http.Handler.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (f RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// Handler is like http.Handle, but specifically for Prometheus query_range calls.
type Handler interface {
	Do(context.Context, Request) (Response, error)
}

// Middleware is a higher order Handler.
type Middleware interface {
	Wrap(Handler) Handler
}

// MiddlewareFunc is like http.HandlerFunc, but for Middleware.
type MiddlewareFunc func(Handler) Handler

// Wrap implements Middleware.
func (q MiddlewareFunc) Wrap(h Handler) Handler {
	return q(h)
}

type roundTripper struct {
	next    http.RoundTripper
	handler Handler
	codec   Codec
	headers []string
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

func NewQueryTripperware(
	log log.Logger,
	registerer prometheus.Registerer,
	forwardHeaders []string,
	queryRangeMiddleware []Middleware,
	instantRangeMiddleware []Middleware,
	queryRangeCodec Codec,
	instantQueryCodec Codec,
	limits Limits,
	queryAnalyzer querysharding.Analyzer,
	defaultSubQueryInterval time.Duration,
	maxSubQuerySteps int64,
	lookbackDelta time.Duration,
	enablePromQLExperimentalFunctions bool,
) Tripperware {

	// The holt_winters function is renamed to double_exponential_smoothing and has been experimental since Prometheus v3. (https://github.com/prometheus/prometheus/pull/14930)
	// The cortex supports holt_winters for users using this function.
	querier.EnableExperimentalPromQLFunctions(enablePromQLExperimentalFunctions, true)

	// Per tenant query metrics.
	queriesPerTenant := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_queries_total",
		Help: "Total queries sent per tenant.",
	}, []string{"op", "source", "user"})

	rejectedQueriesPerTenant := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_rejected_queries_total",
		Help: "Total rejected queries per tenant.",
	}, []string{"op", "user"})

	activeUsers := util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
		err := util.DeleteMatchingLabels(queriesPerTenant, map[string]string{"user": user})
		if err != nil {
			level.Warn(log).Log("msg", "failed to remove cortex_query_frontend_queries_total metric for user", "user", user)
		}
	})

	// Start cleanup. If cleaner stops or fail, we will simply not clean the metrics for inactive users.
	_ = activeUsers.StartAsync(context.Background())
	return func(next http.RoundTripper) http.RoundTripper {
		// Finally, if the user selected any query middleware, stitch it in.
		if len(queryRangeMiddleware) > 0 || len(instantRangeMiddleware) > 0 {
			queryrange := NewRoundTripper(next, queryRangeCodec, forwardHeaders, queryRangeMiddleware...)
			instantQuery := NewRoundTripper(next, instantQueryCodec, forwardHeaders, instantRangeMiddleware...)
			return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
				isQuery := strings.HasSuffix(r.URL.Path, "/query")
				isQueryRange := strings.HasSuffix(r.URL.Path, "/query_range")
				isSeries := strings.HasSuffix(r.URL.Path, "/series")
				isRemoteRead := strings.HasSuffix(r.URL.Path, "/read")
				isLabelNames := strings.HasSuffix(r.URL.Path, "/labels")
				isLabelValues := strings.HasSuffix(r.URL.Path, "/values")
				isMetadata := strings.HasSuffix(r.URL.Path, "/metadata")
				isQueryExemplars := strings.HasSuffix(r.URL.Path, "/query_exemplars")

				op := opTypeQuery
				switch {
				case isQueryRange:
					op = opTypeQueryRange
				case isSeries:
					op = opTypeSeries
				case isRemoteRead:
					op = opTypeRemoteRead
				case isLabelNames:
					op = opTypeLabelNames
				case isLabelValues:
					op = opTypeLabelValues
				case isMetadata:
					op = opTypeMetadata
				case isQueryExemplars:
					op = opTypeQueryExemplars
				}

				tenantIDs, err := tenant.TenantIDs(r.Context())
				// This should never happen anyways because we have auth middleware before this.
				if err != nil {
					return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
				}
				now := time.Now()
				userStr := tenant.JoinTenantIDs(tenantIDs)
				activeUsers.UpdateUserTimestamp(userStr, now)
				source := GetSource(r.Header.Get("User-Agent"))
				queriesPerTenant.WithLabelValues(op, source, userStr).Inc()

				if maxSubQuerySteps > 0 && (isQuery || isQueryRange) {
					query := r.FormValue("query")
					// Check subquery step size.
					if err := SubQueryStepSizeCheck(query, defaultSubQueryInterval, maxSubQuerySteps); err != nil {
						return nil, err
					}
				}

				if err := rejectQueryOrSetPriority(r, now, lookbackDelta, limits, userStr, rejectedQueriesPerTenant); err != nil {
					return nil, err
				}

				if isQueryRange {
					return queryrange.RoundTrip(r)
				} else if isQuery {
					return instantQuery.RoundTrip(r)
				}
				return next.RoundTrip(r)
			})
		}
		return next
	}
}

// NewRoundTripper merges a set of middlewares into an handler, then inject it into the `next` roundtripper
// using the codec to translate requests and responses.
func NewRoundTripper(next http.RoundTripper, codec Codec, headers []string, middlewares ...Middleware) http.RoundTripper {
	transport := roundTripper{
		next:    next,
		codec:   codec,
		headers: headers,
	}
	transport.handler = MergeMiddlewares(middlewares...).Wrap(&transport)
	return transport
}

func (q roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {

	// include the headers specified in the roundTripper during decoding the request.
	request, err := q.codec.DecodeRequest(r.Context(), r, q.headers)
	if err != nil {
		return nil, err
	}

	if span := opentracing.SpanFromContext(r.Context()); span != nil {
		request.LogToSpan(span)
	}

	response, err := q.handler.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return q.codec.EncodeResponse(r.Context(), r, response)
}

// Do implements Handler.
func (q roundTripper) Do(ctx context.Context, r Request) (Response, error) {
	request, err := q.codec.EncodeRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	if headerMap := util_log.HeaderMapFromContext(ctx); headerMap != nil {
		util_log.InjectHeadersIntoHTTPRequest(headerMap, request)
	}

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, request); err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	response, err := q.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
		_ = response.Body.Close()
	}()

	return q.codec.DecodeResponse(ctx, response, r)
}

func GetSource(userAgent string) string {
	if strings.Contains(userAgent, RulerUserAgent) {
		// caller is ruler
		return SourceRuler
	}

	return SourceAPI
}
