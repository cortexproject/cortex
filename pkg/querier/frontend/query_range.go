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

	"github.com/go-kit/kit/log/level"
	"github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"

	client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

var (
	matrix                = model.ValMatrix.String()
	json                  = jsoniter.ConfigCompatibleWithStandardLibrary
	errUnexpectedResponse = httpgrpc.Errorf(http.StatusInternalServerError, "unexpected response type")
)

func parseQueryRangeRequest(r *http.Request) (*QueryRangeRequest, error) {
	var result QueryRangeRequest
	var err error
	result.Start, err = ParseTime(r.FormValue("start"))
	if err != nil {
		return nil, err
	}

	result.End, err = ParseTime(r.FormValue("end"))
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

func ParseTime(s string) (int64, error) {
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

func parseQueryRangeResponse(r *http.Response) (*APIResponse, error) {
	if r.StatusCode/100 != 2 {
		body, _ := ioutil.ReadAll(r.Body)
		return nil, httpgrpc.Errorf(r.StatusCode, string(body))
	}

	var resp APIResponse
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}
	return &resp, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *SampleStream) UnmarshalJSON(data []byte) error {
	var stream struct {
		Metric model.Metric    `json:"metric"`
		Values []client.Sample `json:"values"`
	}
	if err := json.Unmarshal(data, &stream); err != nil {
		return err
	}
	s.Labels = client.ToLabelPairs(stream.Metric)
	s.Samples = stream.Values
	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *SampleStream) MarshalJSON() ([]byte, error) {
	stream := struct {
		Metric model.Metric    `json:"metric"`
		Values []client.Sample `json:"values"`
	}{
		Metric: client.FromLabelPairs(s.Labels),
		Values: s.Samples,
	}
	return json.Marshal(stream)
}

func (a *APIResponse) toHTTPResponse() (*http.Response, error) {
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

func extract(start, end int64, extent Extent) *APIResponse {
	return &APIResponse{
		Status: statusSuccess,
		Data: QueryRangeResponse{
			ResultType: extent.Response.Data.ResultType,
			Result:     extractMatrix(start, end, extent.Response.Data.Result),
		},
	}
}

func extractMatrix(start, end int64, matrix []SampleStream) []SampleStream {
	result := make([]SampleStream, 0, len(matrix))
	for _, stream := range matrix {
		extracted, ok := extractSampleStream(start, end, stream)
		if ok {
			result = append(result, extracted)
		}
	}
	return result
}

func extractSampleStream(start, end int64, stream SampleStream) (SampleStream, bool) {
	result := SampleStream{
		Labels:  stream.Labels,
		Samples: make([]client.Sample, 0, len(stream.Samples)),
	}
	for _, sample := range stream.Samples {
		if start <= sample.TimestampMs && sample.TimestampMs <= end {
			result.Samples = append(result.Samples, sample)
		}
	}
	if len(result.Samples) == 0 {
		return SampleStream{}, false
	}
	return result, true
}

func mergeAPIResponses(responses []*APIResponse) (*APIResponse, error) {
	// Merge the responses.
	sort.Sort(byFirstTime(responses))

	if len(responses) == 0 {
		return &APIResponse{
			Status: statusSuccess,
		}, nil
	}

	return &APIResponse{
		Status: statusSuccess,
		Data: QueryRangeResponse{
			ResultType: model.ValMatrix.String(),
			Result:     matrixMerge(responses),
		},
	}, nil
}

type byFirstTime []*APIResponse

func (a byFirstTime) Len() int           { return len(a) }
func (a byFirstTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFirstTime) Less(i, j int) bool { return minTime(a[i]) < minTime(a[j]) }

func minTime(resp *APIResponse) int64 {
	result := resp.Data.Result
	if len(result) == 0 {
		return -1
	}
	if len(result[0].Samples) == 0 {
		return -1
	}
	return result[0].Samples[0].TimestampMs
}

func matrixMerge(resps []*APIResponse) []SampleStream {
	output := map[string]*SampleStream{}
	for _, resp := range resps {
		for _, stream := range resp.Data.Result {
			metric := client.FromLabelPairsToLabels(stream.Labels).String()
			existing, ok := output[metric]
			if !ok {
				existing = &SampleStream{
					Labels: stream.Labels,
				}
			}
			existing.Samples = append(existing.Samples, stream.Samples...)
			output[metric] = existing
		}
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]SampleStream, 0, len(output))
	for _, key := range keys {
		result = append(result, *output[key])
	}

	return result
}
