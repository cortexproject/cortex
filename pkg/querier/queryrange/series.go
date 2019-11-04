package queryrange

import (
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// ResponseToSamples is needed to map back from api response to the underlying series data
func ResponseToSamples(resp Response) ([]SampleStream, error) {
	promRes, ok := resp.(*PrometheusResponse)
	if !ok {
		return nil, errors.Errorf("error invalid response type: %T, expected: %T", resp, &PrometheusResponse{})
	}
	if promRes.Error != "" {
		return nil, errors.New(promRes.Error)
	}
	switch promRes.Data.ResultType {
	case promql.ValueTypeVector, promql.ValueTypeMatrix:
		return promRes.Data.Result, nil
	}

	return nil, errors.Errorf(
		"Invalid promql.Value type: [%s]. Only %s and %s supported",
		promRes.Data.ResultType,
		promql.ValueTypeVector,
		promql.ValueTypeMatrix,
	)
}

// NewSeriesSet returns an in memory storage.SeriesSet from a []SampleStream
func NewSeriesSet(results []SampleStream) storage.SeriesSet {

	set := make([]storage.Series, 0, len(results))

	for _, stream := range results {
		samples := make([]model.SamplePair, 0, len(stream.Samples))
		for _, sample := range stream.Samples {
			samples = append(samples, model.SamplePair{
				Timestamp: model.Time(sample.TimestampMs),
				Value:     model.SampleValue(sample.Value),
			})
		}

		ls := make([]labels.Label, 0, len(stream.Labels))
		for _, l := range stream.Labels {
			ls = append(ls, labels.Label(l))
		}
		set = append(set, series.NewConcreteSeries(ls, samples))
	}
	return series.NewConcreteSeriesSet(set)
}
