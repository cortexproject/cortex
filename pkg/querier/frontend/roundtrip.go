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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/util/stats"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/util"
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

type queryRangeMiddleware interface {
	Do(context.Context, *queryRangeRequest) (*apiResponse, error)
}

type queryRangeRoundTripper struct {
	downstream           http.RoundTripper
	queryRangeMiddleware queryRangeMiddleware
}

func (q queryRangeRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(r.URL.Path, "/query_range") {
		return q.downstream.RoundTrip(r)
	}

	request, err := parseQueryRangeRequest(r)
	if err != nil {
		return nil, err
	}

	response, err := q.queryRangeMiddleware.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return response.toHTTPResponse()
}

type queryRangeTerminator struct {
	downstream http.RoundTripper
}

func (q queryRangeTerminator) Do(ctx context.Context, r *queryRangeRequest) (*apiResponse, error) {
	request, err := r.toHTTPRequest(ctx)
	if err != nil {
		return nil, err
	}

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, request); err != nil {
		return nil, err
	}

	response, err := q.downstream.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	return parseQueryRangeResponse(response)
}

type queryRangeRequest struct {
	path       string
	start, end int64 // Milliseconds since epoch.
	step       int64 // Milliseconds.
	timeout    time.Duration
	query      string
}

func parseQueryRangeRequest(r *http.Request) (*queryRangeRequest, error) {
	var result queryRangeRequest
	var err error
	result.start, err = parseTime(r.FormValue("start"))
	if err != nil {
		return nil, err
	}

	result.end, err = parseTime(r.FormValue("end"))
	if err != nil {
		return nil, err
	}

	if result.end < result.start {
		return nil, errEndBeforeStart
	}

	result.step, err = parseDurationMs(r.FormValue("step"))
	if err != nil {
		return nil, err
	}

	if result.step <= 0 {
		return nil, errNegativeStep
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (result.end-result.start)/result.step > 11000 {
		return nil, errStepTooSmall
	}

	result.query = r.FormValue("query")
	result.path = r.URL.Path
	return &result, nil
}

func (q queryRangeRequest) toHTTPRequest(ctx context.Context) (*http.Request, error) {
	params := url.Values{
		"start": []string{encodeTime(q.start)},
		"end":   []string{encodeTime(q.end)},
		"step":  []string{encodeDurationMs(q.step)},
		"query": []string{q.query},
	}
	u := &url.URL{
		Path:     q.path,
		RawQuery: params.Encode(),
	}
	req := &http.Request{
		Method:     "GET",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       http.NoBody,
		Header:     http.Header{},
	}

	return req.WithContext(ctx), nil
}

func parseTime(s string) (int64, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		tm := time.Unix(int64(s), int64(ns*float64(time.Second)))
		return tm.UnixNano() / int64(time.Millisecond/time.Nanosecond), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixNano() / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid timestamp", s)
}

func parseDurationMs(s string) (int64, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second/time.Millisecond)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration. It overflows int64", s)
		}
		return int64(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return int64(d) / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration", s)
}

func encodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func encodeDurationMs(d int64) string {
	return strconv.FormatFloat(float64(d)/float64(time.Second/time.Millisecond), 'f', -1, 64)
}

const statusSuccess = "success"

type apiResponse struct {
	Status    string             `json:"status"`
	Data      queryRangeResponse `json:"data,omitempty"`
	ErrorType string             `json:"errorType,omitempty"`
	Error     string             `json:"error,omitempty"`
}

func parseQueryRangeResponse(r *http.Response) (*apiResponse, error) {
	if r.StatusCode/100 != 2 {
		body, _ := ioutil.ReadAll(r.Body)
		return nil, httpgrpc.Errorf(r.StatusCode, string(body))
	}

	var resp apiResponse
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}
	return &resp, nil
}

func (a *apiResponse) toHTTPResponse() (*http.Response, error) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(a)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error marshalling json response", "err", err)
		return nil, err
	}
	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:       ioutil.NopCloser(bytes.NewBuffer(b)),
		StatusCode: http.StatusOK,
	}
	return &resp, nil
}

// queryRangeResponse contains result data for a query_range.
type queryRangeResponse struct {
	ResultType model.ValueType   `json:"resultType"`
	Result     model.Value       `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

func (q *queryRangeResponse) UnmarshalJSON(b []byte) error {
	v := struct {
		ResultType model.ValueType   `json:"resultType"`
		Stats      *stats.QueryStats `json:"stats,omitempty"`
		Result     json.RawMessage   `json:"result"`
	}{}

	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}

	q.ResultType = v.ResultType
	q.Stats = v.Stats

	switch v.ResultType {
	case model.ValVector:
		var vv model.Vector
		err = json.Unmarshal(v.Result, &vv)
		q.Result = vv

	case model.ValMatrix:
		var mv model.Matrix
		err = json.Unmarshal(v.Result, &mv)
		q.Result = mv

	default:
		err = fmt.Errorf("unexpected value type %q", v.ResultType)
	}
	return err
}
