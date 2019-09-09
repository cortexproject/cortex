package validation

import (
	"net/http"
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
)

type validateLabelsCfg struct {
	enforceMetricName      bool
	maxLabelNamesPerSeries int
	maxLabelNameLength     int
	maxLabelValueLength    int
}

func (v validateLabelsCfg) EnforceMetricName(userID string) bool {
	return v.enforceMetricName
}

func (v validateLabelsCfg) MaxLabelNamesPerSeries(userID string) int {
	return v.maxLabelNamesPerSeries
}

func (v validateLabelsCfg) MaxLabelNameLength(userID string) int {
	return v.maxLabelNameLength
}

func (v validateLabelsCfg) MaxLabelValueLength(userID string) int {
	return v.maxLabelValueLength
}

func TestValidateLabels(t *testing.T) {
	var cfg validateLabelsCfg
	userID := "testUser"

	cfg.maxLabelValueLength = 25
	cfg.maxLabelNameLength = 25
	cfg.maxLabelNamesPerSeries = 2
	cfg.enforceMetricName = true

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
			httpgrpc.Errorf(http.StatusBadRequest, errInvalidLabel, "foo ", `valid{foo ="bar"}`),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid"},
			nil,
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelName", "this_is_a_really_really_long_name_that_should_cause_an_error": "test_value_please_ignore"},
			httpgrpc.Errorf(http.StatusBadRequest, errLabelNameTooLong, "this_is_a_really_really_long_name_that_should_cause_an_error", `badLabelName{this_is_a_really_really_long_name_that_should_cause_an_error="test_value_please_ignore"}`),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValue", "much_shorter_name": "test_value_please_ignore_no_really_nothing_to_see_here"},
			httpgrpc.Errorf(http.StatusBadRequest, errLabelValueTooLong, "test_value_please_ignore_no_really_nothing_to_see_here", `badLabelValue{much_shorter_name="test_value_please_ignore_no_really_nothing_to_see_here"}`),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "bar": "baz", "blip": "blop"},
			httpgrpc.Errorf(http.StatusBadRequest, errTooManyLabels, `foo{bar="baz", blip="blop"}`, 3, 2),
		},
	} {

		err := ValidateLabels(cfg, userID, client.FromMetricsToLabelAdapters(c.metric))
		assert.Equal(t, c.err, err, "wrong error")
	}
}
