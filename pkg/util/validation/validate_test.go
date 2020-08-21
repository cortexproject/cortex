package validation

import (
	"net/http"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/ingester/client"
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

type validateMetadataCfg struct {
	enforceMetadataMetricName bool
	maxMetadataLength         int
}

func (vm validateMetadataCfg) EnforceMetadataMetricName(userID string) bool {
	return vm.enforceMetadataMetricName
}

func (vm validateMetadataCfg) MaxMetadataLength(userID string) int {
	return vm.maxMetadataLength
}

func TestValidateLabels(t *testing.T) {
	var cfg validateLabelsCfg
	userID := "testUser"

	cfg.maxLabelValueLength = 25
	cfg.maxLabelNameLength = 25
	cfg.maxLabelNamesPerSeries = 2
	cfg.enforceMetricName = true

	for _, c := range []struct {
		metric                  model.Metric
		skipLabelNameValidation bool
		err                     error
	}{
		{
			map[model.LabelName]model.LabelValue{},
			false,
			httpgrpc.Errorf(http.StatusBadRequest, errMissingMetricName),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: " "},
			false,
			httpgrpc.Errorf(http.StatusBadRequest, errInvalidMetricName, " "),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid", "foo ": "bar"},
			false,
			httpgrpc.Errorf(http.StatusBadRequest, errInvalidLabel, "foo ", `valid{foo ="bar"}`),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "valid"},
			false,
			nil,
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelName", "this_is_a_really_really_long_name_that_should_cause_an_error": "test_value_please_ignore"},
			false,
			httpgrpc.Errorf(http.StatusBadRequest, errLabelNameTooLong, "this_is_a_really_really_long_name_that_should_cause_an_error", `badLabelName{this_is_a_really_really_long_name_that_should_cause_an_error="test_value_please_ignore"}`),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "badLabelValue", "much_shorter_name": "test_value_please_ignore_no_really_nothing_to_see_here"},
			false,
			httpgrpc.Errorf(http.StatusBadRequest, errLabelValueTooLong, "test_value_please_ignore_no_really_nothing_to_see_here", `badLabelValue{much_shorter_name="test_value_please_ignore_no_really_nothing_to_see_here"}`),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "bar": "baz", "blip": "blop"},
			false,
			httpgrpc.Errorf(http.StatusBadRequest, errTooManyLabels, `foo{bar="baz", blip="blop"}`, 3, 2),
		},
		{
			map[model.LabelName]model.LabelValue{model.MetricNameLabel: "foo", "invalid%label&name": "bar"},
			true,
			nil,
		},
	} {

		err := ValidateLabels(cfg, userID, client.FromMetricsToLabelAdapters(c.metric), c.skipLabelNameValidation)
		assert.Equal(t, c.err, err, "wrong error")
	}
}

func TestValidateMetadata(t *testing.T) {
	userID := "testUser"
	var cfg validateMetadataCfg
	cfg.enforceMetadataMetricName = true
	cfg.maxMetadataLength = 22

	for _, c := range []struct {
		desc     string
		metadata *client.MetricMetadata
		err      error
	}{
		{
			"with a valid config",
			&client.MetricMetadata{MetricName: "go_goroutines", Type: client.COUNTER, Help: "Number of goroutines.", Unit: ""},
			nil,
		},
		{
			"with no metric name",
			&client.MetricMetadata{MetricName: "", Type: client.COUNTER, Help: "Number of goroutines.", Unit: ""},
			httpgrpc.Errorf(http.StatusBadRequest, "metadata missing metric name"),
		},
		{
			"with a long metric name",
			&client.MetricMetadata{MetricName: "go_goroutines_and_routines_and_routines", Type: client.COUNTER, Help: "Number of goroutines.", Unit: ""},
			httpgrpc.Errorf(http.StatusBadRequest, "metadata 'METRIC_NAME' value too long: \"go_goroutines_and_routines_and_routines\" metric \"go_goroutines_and_routines_and_routines\""),
		},
		{
			"with a long help",
			&client.MetricMetadata{MetricName: "go_goroutines", Type: client.COUNTER, Help: "Number of goroutines that currently exist.", Unit: ""},
			httpgrpc.Errorf(http.StatusBadRequest, "metadata 'HELP' value too long: \"Number of goroutines that currently exist.\" metric \"go_goroutines\""),
		},
		{
			"with a long unit",
			&client.MetricMetadata{MetricName: "go_goroutines", Type: client.COUNTER, Help: "Number of goroutines.", Unit: "a_made_up_unit_that_is_really_long"},
			httpgrpc.Errorf(http.StatusBadRequest, "metadata 'UNIT' value too long: \"a_made_up_unit_that_is_really_long\" metric \"go_goroutines\""),
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			err := ValidateMetadata(cfg, userID, c.metadata)
			assert.Equal(t, c.err, err, "wrong error")
		})
	}
}

func TestValidateLabelOrder(t *testing.T) {
	var cfg validateLabelsCfg
	cfg.maxLabelNameLength = 10
	cfg.maxLabelNamesPerSeries = 10
	cfg.maxLabelValueLength = 10

	userID := "testUser"

	err := ValidateLabels(cfg, userID, []client.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "m"},
		{Name: "b", Value: "b"},
		{Name: "a", Value: "a"},
	}, false)
	assert.Equal(t, httpgrpc.Errorf(http.StatusBadRequest, errLabelsNotSorted, "a", `m{b="b", a="a"}`), err)
}

func TestValidateLabelDuplication(t *testing.T) {
	var cfg validateLabelsCfg
	cfg.maxLabelNameLength = 10
	cfg.maxLabelNamesPerSeries = 10
	cfg.maxLabelValueLength = 10

	userID := "testUser"

	err := ValidateLabels(cfg, userID, []client.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: model.MetricNameLabel, Value: "b"},
	}, false)
	assert.Equal(t, httpgrpc.Errorf(http.StatusBadRequest, errDuplicateLabelName, "__name__", `a{__name__="b"}`), err)

	err = ValidateLabels(cfg, userID, []client.LabelAdapter{
		{Name: model.MetricNameLabel, Value: "a"},
		{Name: "a", Value: "a"},
		{Name: "a", Value: "a"},
	}, false)
	assert.Equal(t, httpgrpc.Errorf(http.StatusBadRequest, errDuplicateLabelName, "a", `a{a="a", a="a"}`), err)
}
