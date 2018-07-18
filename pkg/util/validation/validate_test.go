package validation

import (
	"net/http"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util"
)

func TestValidateLabels(t *testing.T) {
	var cfg Config
	util.DefaultValues(&cfg)
	cfg.MaxLabelValueLength = 25
	cfg.MaxLabelNameLength = 25
	cfg.MaxLabelNamesPerSeries = 64

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
			httpgrpc.Errorf(http.StatusBadRequest, errInvalidLabel, "foo ", "valid"),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid"},
			nil,
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelName", "this_is_a_really_really_long_name_that_should_cause_an_error": "test_value_please_ignore"},
			httpgrpc.Errorf(http.StatusBadRequest, errLabelNameTooLong, 60, "this_is_a_really_really_long_name_that_should_cause_an_error", "badLabelName"),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValue", "much_shorter_name": "test_value_please_ignore_no_really_nothing_to_see_here"},
			httpgrpc.Errorf(http.StatusBadRequest, errLabelValueTooLong, 54, "test_value_please_ignore_no_really_nothing_to_see_here", "badLabelValue"),
		},
	} {

		err := cfg.ValidateLabels(client.ToLabelPairs(c.metric))
		assert.Equal(t, c.err, err, "wrong error")
	}
}
