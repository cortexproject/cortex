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

package frontend

import (
	"context"
	"net/http"
	"strings"

	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
)

var (
	errEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "end timestamp must not be before start time")
	errNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "zero or negative query resolution step widths are not accepted. Try a positive integer")
	errStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
)

// RoundTripperFunc is like http.HandlerFunc, but for http.RoundTripper.
type RoundTripperFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (fn RoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

// queryRangeHandlerFunc is like http.HandlerFunc, but for queryRangeHandler.
type queryRangeHandlerFunc func(context.Context, *QueryRangeRequest) (*APIResponse, error)

func (q queryRangeHandlerFunc) Do(ctx context.Context, req *QueryRangeRequest) (*APIResponse, error) {
	return q(ctx, req)
}

type queryRangeHandler interface {
	Do(context.Context, *QueryRangeRequest) (*APIResponse, error)
}

// queryRangeMiddlewareFunc is like http.HandlerFunc, but for queryRangeMiddleware.
type queryRangeMiddlewareFunc func(queryRangeHandler) queryRangeHandler

func (q queryRangeMiddlewareFunc) Wrap(h queryRangeHandler) queryRangeHandler {
	return q(h)
}

type queryRangeMiddleware interface {
	Wrap(queryRangeHandler) queryRangeHandler
}

// merge produces a middleware that applies multiple middlesware in turn;
// ie Merge(f,g,h).Wrap(handler) == f.Wrap(g.Wrap(h.Wrap(handler)))
func merge(middlesware ...queryRangeMiddleware) queryRangeMiddleware {
	return queryRangeMiddlewareFunc(func(next queryRangeHandler) queryRangeHandler {
		for i := len(middlesware) - 1; i >= 0; i-- {
			next = middlesware[i].Wrap(next)
		}
		return next
	})
}

type queryRangeRoundTripper struct {
	next                 http.RoundTripper
	queryRangeMiddleware queryRangeHandler
	limits               *validation.Overrides
}

func (q queryRangeRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(r.URL.Path, "/query_range") {
		return q.next.RoundTrip(r)
	}

	request, err := parseQueryRangeRequest(r)
	if err != nil {
		return nil, err
	}
	request.logToSpan(r.Context())

	userid, err := user.ExtractOrgID(r.Context())
	if err != nil {
		return nil, err
	}

	maxQueryLen := q.limits.MaxQueryLength(userid)
	queryLen := timestamp.Time(request.End).Sub(timestamp.Time(request.Start))
	if maxQueryLen != 0 && queryLen > maxQueryLen {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, validation.ErrQueryTooLong, queryLen, maxQueryLen)
	}

	response, err := q.queryRangeMiddleware.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return response.toHTTPResponse(r.Context())
}

type queryRangeTerminator struct {
	next     http.RoundTripper
	nextGRPC interface {
		RoundTripGRPC(ctx context.Context, req *ProcessRequest) (*ProcessResponse, error)
	}
}

func (q queryRangeTerminator) Do(ctx context.Context, r *QueryRangeRequest) (*APIResponse, error) {
	request, err := r.toHTTPRequest(ctx)
	if err != nil {
		return nil, err
	}

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, request); err != nil {
		return nil, err
	}

	r.logToSpan(ctx)
	response, err := q.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	return parseQueryRangeResponse(ctx, response)
}
