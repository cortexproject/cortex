package util

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/weaveworks/cortex"
)

// FromWriteRequest converts a WriteRequest proto into an array of samples.
func FromWriteRequest(req *remote.WriteRequest) []*model.Sample {
	var samples []*model.Sample
	for _, ts := range req.Timeseries {
		metric := model.Metric{}
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.TimestampMs),
			})
		}
	}
	return samples
}

// ToWriteRequest converts an array of samples into a WriteRequest proto.
func ToWriteRequest(samples []*model.Sample) *remote.WriteRequest {
	req := &remote.WriteRequest{
		Timeseries: make([]*remote.TimeSeries, 0, len(samples)),
	}

	for _, s := range samples {
		ts := &remote.TimeSeries{
			Labels: make([]*remote.LabelPair, 0, len(s.Metric)),
		}
		for k, v := range s.Metric {
			ts.Labels = append(ts.Labels,
				&remote.LabelPair{
					Name:  string(k),
					Value: string(v),
				})
		}
		ts.Samples = []*remote.Sample{
			{
				Value:       float64(s.Value),
				TimestampMs: int64(s.Timestamp),
			},
		}
		req.Timeseries = append(req.Timeseries, ts)
	}

	return req
}

// ToQueryRequest builds a QueryRequest proto.
func ToQueryRequest(from, to model.Time, matchers []*metric.LabelMatcher) (*cortex.QueryRequest, error) {
	ms, err := toLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	return &cortex.QueryRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         ms,
	}, nil
}

// FromQueryRequest unpacks a QueryRequest proto.
func FromQueryRequest(req *cortex.QueryRequest) (model.Time, model.Time, []*metric.LabelMatcher, error) {
	matchers, err := fromLabelMatchers(req.Matchers)
	if err != nil {
		return 0, 0, nil, err
	}
	from := model.Time(req.StartTimestampMs)
	to := model.Time(req.EndTimestampMs)
	return from, to, matchers, nil
}

// ToQueryResponse builds a QueryResponse proto.
func ToQueryResponse(matrix model.Matrix) *cortex.QueryResponse {
	resp := &cortex.QueryResponse{}
	for _, ss := range matrix {
		ts := &remote.TimeSeries{
			Labels:  toLabelPairs(ss.Metric),
			Samples: make([]*remote.Sample, 0, len(ss.Values)),
		}
		for _, s := range ss.Values {
			ts.Samples = append(ts.Samples, &remote.Sample{
				Value:       float64(s.Value),
				TimestampMs: int64(s.Timestamp),
			})
		}
		resp.Timeseries = append(resp.Timeseries, ts)
	}
	return resp
}

// FromQueryResponse unpacks a QueryResponse proto.
func FromQueryResponse(resp *cortex.QueryResponse) model.Matrix {
	m := make(model.Matrix, 0, len(resp.Timeseries))
	for _, ts := range resp.Timeseries {
		var ss model.SampleStream
		ss.Metric = fromLabelPairs(ts.Labels)
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
func ToMetricsForLabelMatchersRequest(from, to model.Time, matchersSet []metric.LabelMatchers) (*cortex.MetricsForLabelMatchersRequest, error) {
	req := &cortex.MetricsForLabelMatchersRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		MatchersSet:      make([]*cortex.LabelMatchers, 0, len(matchersSet)),
	}

	for _, matchers := range matchersSet {
		ms, err := toLabelMatchers(matchers)
		if err != nil {
			return nil, err
		}
		req.MatchersSet = append(req.MatchersSet, &cortex.LabelMatchers{
			Matchers: ms,
		})
	}
	return req, nil
}

// FromMetricsForLabelMatchersRequest unpacks a MetricsForLabelMatchersRequest proto
func FromMetricsForLabelMatchersRequest(req *cortex.MetricsForLabelMatchersRequest) (model.Time, model.Time, []metric.LabelMatchers, error) {
	matchersSet := make([]metric.LabelMatchers, 0, len(req.MatchersSet))
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
func ToMetricsForLabelMatchersResponse(metrics []model.Metric) *cortex.MetricsForLabelMatchersResponse {
	resp := &cortex.MetricsForLabelMatchersResponse{
		Metric: make([]*cortex.Metric, 0, len(metrics)),
	}
	for _, metric := range metrics {
		resp.Metric = append(resp.Metric, &cortex.Metric{
			Labels: toLabelPairs(metric),
		})
	}
	return resp
}

// FromMetricsForLabelMatchersResponse unpacks a MetricsForLabelMatchersResponse proto
func FromMetricsForLabelMatchersResponse(resp *cortex.MetricsForLabelMatchersResponse) []model.Metric {
	metrics := []model.Metric{}
	for _, m := range resp.Metric {
		metrics = append(metrics, fromLabelPairs(m.Labels))
	}
	return metrics
}

func toLabelMatchers(matchers []*metric.LabelMatcher) ([]*cortex.LabelMatcher, error) {
	result := make([]*cortex.LabelMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mType cortex.MatchType
		switch matcher.Type {
		case metric.Equal:
			mType = cortex.MatchType_EQUAL
		case metric.NotEqual:
			mType = cortex.MatchType_NOT_EQUAL
		case metric.RegexMatch:
			mType = cortex.MatchType_REGEX_MATCH
		case metric.RegexNoMatch:
			mType = cortex.MatchType_REGEX_NO_MATCH
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		result = append(result, &cortex.LabelMatcher{
			Type:  mType,
			Name:  string(matcher.Name),
			Value: string(matcher.Value),
		})
	}
	return result, nil
}

func fromLabelMatchers(matchers []*cortex.LabelMatcher) ([]*metric.LabelMatcher, error) {
	result := make(metric.LabelMatchers, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype metric.MatchType
		switch matcher.Type {
		case cortex.MatchType_EQUAL:
			mtype = metric.Equal
		case cortex.MatchType_NOT_EQUAL:
			mtype = metric.NotEqual
		case cortex.MatchType_REGEX_MATCH:
			mtype = metric.RegexMatch
		case cortex.MatchType_REGEX_NO_MATCH:
			mtype = metric.RegexNoMatch
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		matcher, err := metric.NewLabelMatcher(mtype, model.LabelName(matcher.Name), model.LabelValue(matcher.Value))
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}

func toLabelPairs(metric model.Metric) []*remote.LabelPair {
	labelPairs := make([]*remote.LabelPair, 0, len(metric))
	for k, v := range metric {
		labelPairs = append(labelPairs, &remote.LabelPair{
			Name:  string(k),
			Value: string(v),
		})
	}
	return labelPairs
}

func fromLabelPairs(labelPairs []*remote.LabelPair) model.Metric {
	metric := model.Metric{}
	for _, l := range labelPairs {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}
