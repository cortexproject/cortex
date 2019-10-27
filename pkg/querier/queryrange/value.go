package queryrange

import (
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

// FromResult transforms a promql query result into a samplestream
func FromResult(res *promql.Result) ([]SampleStream, error) {
	if res.Err != nil {
		return nil, res.Err
	}
	switch v := res.Value.(type) {
	case promql.Scalar:
		return []SampleStream{
			{
				Samples: []client.Sample{
					{
						Value:       v.V,
						TimestampMs: v.T,
					},
				},
			},
		}, nil

	case promql.Vector:
		res := make([]SampleStream, 0, len(v))
		for _, sample := range v {
			res = append(res, SampleStream{
				Labels:  mapLabels(sample.Metric),
				Samples: mapPoints(sample.Point),
			})
		}
		return res, nil

	case promql.Matrix:
		res := make([]SampleStream, 0, len(v))
		for _, series := range v {
			res = append(res, SampleStream{
				Labels:  mapLabels(series.Metric),
				Samples: mapPoints(series.Points...),
			})
		}
		return res, nil

	}

	return nil, errors.Errorf("Unexpected value type: [%s]", res.Value.Type())
}

func mapLabels(ls labels.Labels) []client.LabelAdapter {
	result := make([]client.LabelAdapter, 0, len(ls))
	for _, l := range ls {
		result = append(result, client.LabelAdapter(l))
	}

	return result
}

func mapPoints(pts ...promql.Point) []client.Sample {
	result := make([]client.Sample, 0, len(pts))

	for _, pt := range pts {
		result = append(result, client.Sample{
			Value:       pt.V,
			TimestampMs: pt.T,
		})
	}

	return result
}
