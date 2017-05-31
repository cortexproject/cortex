package util

import (
	"net/http"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
)

func TestValidate(t *testing.T) {
	for _, c := range []struct {
		metric model.Metric
		err    error
	}{
		{
			map[model.LabelName]model.LabelValue{},
			httpgrpc.Errorf(http.StatusBadRequest, errMissingMetricName),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: " "},
			httpgrpc.Errorf(http.StatusBadRequest, errInvalidMetricName, " "),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid", "foo ": "bar"},
			httpgrpc.Errorf(http.StatusBadRequest, errInvalidLabel, "foo "),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid"},
			nil,
		},
	} {
		err := ValidateSample(&model.Sample{
			Metric: c.metric,
		})
		assert.Equal(t, c.err, err, "wrong error")
	}
}
