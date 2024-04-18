package queryrange

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
)

// FromResult transforms a promql query result into a samplestream
func FromResult(res *promql.Result) ([]tripperware.SampleStream, error) {
	if res.Err != nil {
		// The error could be wrapped by the PromQL engine. We get the error's cause in order to
		// correctly parse the error in parent callers (eg. gRPC response status code extraction).
		return nil, errors.Cause(res.Err)
	}
	switch v := res.Value.(type) {
	case promql.Scalar:
		return []tripperware.SampleStream{
			{
				Samples: []cortexpb.Sample{
					{
						Value:       v.V,
						TimestampMs: v.T,
					},
				},
			},
		}, nil

	case promql.Vector:
		res := make([]tripperware.SampleStream, 0, len(v))
		for _, sample := range v {
			res = append(res, tripperware.SampleStream{
				Labels: mapLabels(sample.Metric),
				Samples: mapPoints(promql.FPoint{
					T: sample.T,
					F: sample.F,
				}),
			})
		}
		return res, nil

	case promql.Matrix:
		res := make([]tripperware.SampleStream, 0, len(v))
		for _, series := range v {
			res = append(res, tripperware.SampleStream{
				Labels:  mapLabels(series.Metric),
				Samples: mapPoints(series.Floats...),
			})
		}
		return res, nil

	}

	return nil, errors.Errorf("Unexpected value type: [%s]", res.Value.Type())
}

func mapLabels(ls labels.Labels) []cortexpb.LabelAdapter {
	result := make([]cortexpb.LabelAdapter, 0, len(ls))
	for _, l := range ls {
		result = append(result, cortexpb.LabelAdapter(l))
	}

	return result
}

func mapPoints(pts ...promql.FPoint) []cortexpb.Sample {
	result := make([]cortexpb.Sample, 0, len(pts))

	for _, pt := range pts {
		result = append(result, cortexpb.Sample{
			Value:       pt.F,
			TimestampMs: pt.T,
		})
	}

	return result
}
