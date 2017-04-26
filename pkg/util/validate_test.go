package util

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	for _, c := range []struct {
		metric model.Metric
		err    error
	}{
		{map[model.LabelName]model.LabelValue{}, ErrMissingMetricName},
		{map[model.LabelName]model.LabelValue{model.MetricNameLabel: " "}, ErrInvalidMetricName},
		{map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid", "foo ": "bar"}, ErrInvalidLabel},
		{map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid"}, nil},
	} {
		err := ValidateSample(&model.Sample{
			Metric: c.metric,
		})
		assert.Equal(t, c.err, err, "wrong error")
	}
}
