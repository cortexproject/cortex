package util

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/cortex/pkg/ingester/client"
)

// FromWriteRequest converts a WriteRequest proto into an array of samples.
func FromWriteRequest(req *client.WriteRequest) []model.Sample {
	// Just guess that there is one sample per timeseries
	samples := make([]model.Sample, 0, len(req.Timeseries))
	for _, ts := range req.Timeseries {
		for _, s := range ts.Samples {
			samples = append(samples, model.Sample{
				Metric:    FromLabelPairs(ts.Labels),
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.TimestampMs),
			})
		}
	}
	return samples
}

// ToWriteRequest converts an array of samples into a WriteRequest proto.
func ToWriteRequest(samples []model.Sample) *client.WriteRequest {
	req := &client.WriteRequest{
		Timeseries: make([]client.TimeSeries, 0, len(samples)),
	}

	for _, s := range samples {
		ts := client.TimeSeries{
			Labels: ToLabelPairs(s.Metric),
			Samples: []client.Sample{
				{
					Value:       float64(s.Value),
					TimestampMs: int64(s.Timestamp),
				},
			},
		}
		req.Timeseries = append(req.Timeseries, ts)
	}

	return req
}

// ToQueryRequest builds a QueryRequest proto.
func ToQueryRequest(from, to model.Time, matchers []*labels.Matcher) (*client.QueryRequest, error) {
	ms, err := toLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	return &client.QueryRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         ms,
	}, nil
}

// FromQueryRequest unpacks a QueryRequest proto.
func FromQueryRequest(req *client.QueryRequest) (model.Time, model.Time, []*labels.Matcher, error) {
	matchers, err := fromLabelMatchers(req.Matchers)
	if err != nil {
		return 0, 0, nil, err
	}
	from := model.Time(req.StartTimestampMs)
	to := model.Time(req.EndTimestampMs)
	return from, to, matchers, nil
}

// ToQueryResponse builds a QueryResponse proto.
func ToQueryResponse(matrix model.Matrix) *client.QueryResponse {
	resp := &client.QueryResponse{}
	for _, ss := range matrix {
		ts := client.TimeSeries{
			Labels:  ToLabelPairs(ss.Metric),
			Samples: make([]client.Sample, 0, len(ss.Values)),
		}
		for _, s := range ss.Values {
			ts.Samples = append(ts.Samples, client.Sample{
				Value:       float64(s.Value),
				TimestampMs: int64(s.Timestamp),
			})
		}
		resp.Timeseries = append(resp.Timeseries, ts)
	}
	return resp
}

// FromQueryResponse unpacks a QueryResponse proto.
func FromQueryResponse(resp *client.QueryResponse) model.Matrix {
	m := make(model.Matrix, 0, len(resp.Timeseries))
	for _, ts := range resp.Timeseries {
		var ss model.SampleStream
		ss.Metric = FromLabelPairs(ts.Labels)
		ss.Values = make([]model.SamplePair, 0, len(ts.Samples))
		for _, s := range ts.Samples {
			ss.Values = append(ss.Values, model.SamplePair{
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.TimestampMs),
			})
		}
		m = append(m, &ss)
	}

	return m
}

// ToMetricsForLabelMatchersRequest builds a MetricsForLabelMatchersRequest proto
func ToMetricsForLabelMatchersRequest(from, to model.Time, matchers []*labels.Matcher) (*client.MetricsForLabelMatchersRequest, error) {
	ms, err := toLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	return &client.MetricsForLabelMatchersRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		MatchersSet:      []*client.LabelMatchers{{Matchers: ms}},
	}, nil
}

// FromMetricsForLabelMatchersRequest unpacks a MetricsForLabelMatchersRequest proto
func FromMetricsForLabelMatchersRequest(req *client.MetricsForLabelMatchersRequest) (model.Time, model.Time, [][]*labels.Matcher, error) {
	matchersSet := make([][]*labels.Matcher, 0, len(req.MatchersSet))
	for _, matchers := range req.MatchersSet {
		matchers, err := fromLabelMatchers(matchers.Matchers)
		if err != nil {
			return 0, 0, nil, err
		}
		matchersSet = append(matchersSet, matchers)
	}
	from := model.Time(req.StartTimestampMs)
	to := model.Time(req.EndTimestampMs)
	return from, to, matchersSet, nil
}

// ToMetricsForLabelMatchersResponse builds a MetricsForLabelMatchersResponse proto
func ToMetricsForLabelMatchersResponse(metrics []model.Metric) *client.MetricsForLabelMatchersResponse {
	resp := &client.MetricsForLabelMatchersResponse{
		Metric: make([]*client.Metric, 0, len(metrics)),
	}
	for _, metric := range metrics {
		resp.Metric = append(resp.Metric, &client.Metric{
			Labels: ToLabelPairs(metric),
		})
	}
	return resp
}

// FromMetricsForLabelMatchersResponse unpacks a MetricsForLabelMatchersResponse proto
func FromMetricsForLabelMatchersResponse(resp *client.MetricsForLabelMatchersResponse) []model.Metric {
	metrics := []model.Metric{}
	for _, m := range resp.Metric {
		metrics = append(metrics, FromLabelPairs(m.Labels))
	}
	return metrics
}

func toLabelMatchers(matchers []*labels.Matcher) ([]*client.LabelMatcher, error) {
	result := make([]*client.LabelMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mType client.MatchType
		switch matcher.Type {
		case labels.MatchEqual:
			mType = client.EQUAL
		case labels.MatchNotEqual:
			mType = client.NOT_EQUAL
		case labels.MatchRegexp:
			mType = client.REGEX_MATCH
		case labels.MatchNotRegexp:
			mType = client.REGEX_NO_MATCH
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		result = append(result, &client.LabelMatcher{
			Type:  mType,
			Name:  string(matcher.Name),
			Value: string(matcher.Value),
		})
	}
	return result, nil
}

func fromLabelMatchers(matchers []*client.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype labels.MatchType
		switch matcher.Type {
		case client.EQUAL:
			mtype = labels.MatchEqual
		case client.NOT_EQUAL:
			mtype = labels.MatchNotEqual
		case client.REGEX_MATCH:
			mtype = labels.MatchRegexp
		case client.REGEX_NO_MATCH:
			mtype = labels.MatchNotRegexp
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		matcher, err := labels.NewMatcher(mtype, matcher.Name, matcher.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}

// ToLabelPairs builds a []client.LabelPair from a model.Metric
func ToLabelPairs(metric model.Metric) []client.LabelPair {
	labelPairs := make([]client.LabelPair, 0, len(metric))
	for k, v := range metric {
		labelPairs = append(labelPairs, client.LabelPair{
			Name:  []byte(k),
			Value: []byte(v),
		})
	}
	return labelPairs
}

// FromLabelPairs unpack a []client.LabelPair to a model.Metric
func FromLabelPairs(labelPairs []client.LabelPair) model.Metric {
	metric := make(model.Metric, len(labelPairs))
	for _, l := range labelPairs {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}
