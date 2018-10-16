package frontend

import (
	"bytes"
	"context"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/weaveworks/common/httpgrpc"
)

var (
	json                  = jsoniter.ConfigCompatibleWithStandardLibrary
	errUnexpectedResponse = httpgrpc.Errorf(http.StatusInternalServerError, "unexpected response type")
)

func parseQueryRangeRequest(r *http.Request) (*QueryRangeRequest, error) {
	var result QueryRangeRequest
	var err error
	result.Start, err = parseTime(r.FormValue("start"))
	if err != nil {
		return nil, err
	}

	result.End, err = parseTime(r.FormValue("end"))
	if err != nil {
		return nil, err
	}

	if result.End < result.Start {
		return nil, errEndBeforeStart
	}

	result.Step, err = parseDurationMs(r.FormValue("step"))
	if err != nil {
		return nil, err
	}

	if result.Step <= 0 {
		return nil, errNegativeStep
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (result.End-result.Start)/result.Step > 11000 {
		return nil, errStepTooSmall
	}

	result.Query = r.FormValue("query")
	result.Path = r.URL.Path
	return &result, nil
}

func (q QueryRangeRequest) copy() QueryRangeRequest {
	return q
}

func (q QueryRangeRequest) toHTTPRequest(ctx context.Context) (*http.Request, error) {
	params := url.Values{
		"start": []string{encodeTime(q.Start)},
		"end":   []string{encodeTime(q.End)},
		"step":  []string{encodeDurationMs(q.Step)},
		"query": []string{q.Query},
	}
	u := &url.URL{
		Path:     q.Path,
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
		ResultType model.ValueType     `json:"resultType"`
		Stats      *stats.QueryStats   `json:"stats,omitempty"`
		Result     jsoniter.RawMessage `json:"result"`
	}{}

	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	q.ResultType = v.ResultType
	q.Stats = v.Stats

	switch v.ResultType {
	case model.ValVector:
		var vv model.Vector
		if err := json.Unmarshal(v.Result, &vv); err != nil {
			return err
		}
		q.Result = vv

	case model.ValMatrix:
		var mv model.Matrix
		if err := json.Unmarshal(v.Result, &mv); err != nil {
			return err
		}
		q.Result = mv

	default:
		return errUnexpectedResponse
	}

	return nil
}

func extract(start, end int64, extent extent) *apiResponse {
	resp := &apiResponse{
		Status: statusSuccess,
		Data: queryRangeResponse{
			ResultType: extent.Response.Data.ResultType,
		},
	}
	switch data := extent.Response.Data.Result.(type) {
	case model.Vector:
		resp.Data.Result = extractVector(start, end, data)
	case model.Matrix:
		resp.Data.Result = extractMatrix(start, end, data)
	default:
		panic("?")
	}
	return resp
}

func extractVector(start, end int64, vector model.Vector) model.Vector {
	result := model.Vector{}
	for _, sample := range vector {
		if model.Time(start) <= sample.Timestamp || sample.Timestamp <= model.Time(end) {
			result = append(result, sample)
		}
	}
	return result
}

func extractMatrix(start, end int64, matrix model.Matrix) model.Matrix {
	result := make(model.Matrix, 0, len(matrix))
	for _, stream := range matrix {
		extracted := extractSampleStream(start, end, stream)
		if extracted != nil {
			result = append(result, extracted)
		}
	}
	return result
}

func extractSampleStream(start, end int64, stream *model.SampleStream) *model.SampleStream {
	result := &model.SampleStream{
		Metric: stream.Metric,
	}
	for _, sample := range stream.Values {
		if model.Time(start) <= sample.Timestamp && sample.Timestamp <= model.Time(end) {
			result.Values = append(result.Values, sample)
		}
	}
	if len(result.Values) == 0 {
		return nil
	}
	return result
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
		return nil, errUnexpectedResponse
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
