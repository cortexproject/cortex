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
func ToQueryRequest(from, to model.Time, matchers ...*metric.LabelMatcher) (*cortex.QueryRequest, error) {
	req := &cortex.QueryRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
	}
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
		req.Matchers = append(req.Matchers, &cortex.LabelMatcher{
			Type:  mType,
			Name:  string(matcher.Name),
			Value: string(matcher.Value),
		})
	}
	return req, nil
}

// FromQueryRequest unpacks a QueryRequest proto.
func FromQueryRequest(req *cortex.QueryRequest) (model.Time, model.Time, []*metric.LabelMatcher, error) {
	matchers := make(metric.LabelMatchers, 0, len(req.Matchers))
	for _, matcher := range req.Matchers {
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
			return 0, 0, nil, fmt.Errorf("invalid matcher type")
		}
		matcher, err := metric.NewLabelMatcher(mtype, model.LabelName(matcher.Name), model.LabelValue(matcher.Value))
		if err != nil {
			return 0, 0, nil, err
		}
		matchers = append(matchers, matcher)
	}

	from := model.Time(req.StartTimestampMs)
	to := model.Time(req.EndTimestampMs)
	return from, to, matchers, nil
}

// ToQueryResponse builds a QueryResponse proto.
func ToQueryResponse(matrix model.Matrix) *cortex.QueryResponse {
	resp := &cortex.QueryResponse{}
	for _, ss := range matrix {
		ts := &remote.TimeSeries{}
		for k, v := range ss.Metric {
			ts.Labels = append(ts.Labels,
				&remote.LabelPair{
					Name:  string(k),
					Value: string(v),
				})
		}
		ts.Samples = make([]*remote.Sample, 0, len(ss.Values))
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
		ss.Metric = model.Metric{}
		for _, l := range ts.Labels {
			ss.Metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

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
