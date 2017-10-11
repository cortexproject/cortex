package ingester

import (
	"net/http"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
)

func TestValidate(t *testing.T) {
	cfg := ValidateConfig{
		MaxLabelValueLength:    25,
		MaxLabelNameLength:     25,
		MaxLabelNamesPerSeries: 64,
	}
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
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelName", "this_is_a_really_really_long_name_that_should_cause_an_error": "test_value_please_ignore"},
			httpgrpc.Errorf(http.StatusBadRequest, errLabelNameTooLong, "this_is_a_really_really_long_name_that_should_cause_an_error"),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValue", "much_shorter_name": "test_value_please_ignore_no_really_nothing_to_see_here"},
			httpgrpc.Errorf(http.StatusBadRequest, errLabelValueTooLong, "test_value_please_ignore_no_really_nothing_to_see_here"),
		},
	} {
		err := ValidateSample(&model.Sample{
			Metric: c.metric,
		}, &cfg)
		assert.Equal(t, c.err, err, "wrong error")
	}
}
