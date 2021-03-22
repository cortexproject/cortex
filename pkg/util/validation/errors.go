package validation

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// ValidationError is an error returned by series validation. It provides a function to convert
// the error into an HTTPGRPC error.
//
// nolint:golint ignore stutter warning
type ValidationError interface {
	error

	// ToHTTPGRPCError returns the httpgrpc version of the error.
	ToHTTPGRPCError() error
}

// genericValidationError is a basic implementation of ValidationError which can be used when the
// error format only contains the cause and the series.
type genericValidationError struct {
	message string
	cause   string
	series  []cortexpb.LabelAdapter
}

func (e *genericValidationError) ToHTTPGRPCError() error {
	return httpgrpc.Errorf(http.StatusBadRequest, e.message, e.cause, formatLabelSet(e.series))
}

func (e *genericValidationError) Error() string {
	return e.ToHTTPGRPCError().Error()
}

func newLabelNameTooLongError(series []cortexpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "label name too long: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newLabelValueTooLongError(series []cortexpb.LabelAdapter, labelValue string) ValidationError {
	return &genericValidationError{
		message: "label value too long: %.200q metric %.200q",
		cause:   labelValue,
		series:  series,
	}
}

func newInvalidLabelError(series []cortexpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "sample invalid label: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newDuplicatedLabelError(series []cortexpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "duplicate label name: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newLabelsNotSortedError(series []cortexpb.LabelAdapter, labelName string) ValidationError {
	return &genericValidationError{
		message: "labels not sorted: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

type tooManyLabelsError struct {
	series []cortexpb.LabelAdapter
	limit  int
}

func newTooManyLabelsError(series []cortexpb.LabelAdapter, limit int) ValidationError {
	return &tooManyLabelsError{
		series: series,
		limit:  limit,
	}
}

func (e *tooManyLabelsError) ToHTTPGRPCError() error {
	return httpgrpc.Errorf(
		http.StatusBadRequest,
		"series has too many labels (actual: %d, limit: %d) series: '%s'",
		len(e.series), e.limit, cortexpb.FromLabelAdaptersToMetric(e.series).String())
}

func (e *tooManyLabelsError) Error() string {
	return e.ToHTTPGRPCError().Error()
}

type noMetricNameError struct{}

func newNoMetricNameError() ValidationError {
	return &noMetricNameError{}
}

func (e *noMetricNameError) ToHTTPGRPCError() error {
	return httpgrpc.Errorf(http.StatusBadRequest, "sample missing metric name")
}

func (e *noMetricNameError) Error() string {
	return e.ToHTTPGRPCError().Error()
}

type invalidMetricNameError struct {
	metricName string
}

func newInvalidMetricNameError(metricName string) ValidationError {
	return &invalidMetricNameError{
		metricName: metricName,
	}
}

func (e *invalidMetricNameError) ToHTTPGRPCError() error {
	return httpgrpc.Errorf(http.StatusBadRequest, "sample invalid metric name: %.200q", e.metricName)
}

func (e *invalidMetricNameError) Error() string {
	return e.ToHTTPGRPCError().Error()
}

// sampleValidationError is a ValidationError implementation suitable for sample validation errors.
type sampleValidationError struct {
	message    string
	metricName string
	timestamp  int64
}

func (e *sampleValidationError) ToHTTPGRPCError() error {
	return httpgrpc.Errorf(http.StatusBadRequest, e.message, e.timestamp, e.metricName)
}

func (e *sampleValidationError) Error() string {
	return e.ToHTTPGRPCError().Error()
}

func newSampleTimestampTooOldError(metricName string, timestamp int64) ValidationError {
	return &sampleValidationError{
		message:    "timestamp too old: %d metric: %.200q",
		metricName: metricName,
		timestamp:  timestamp,
	}
}

func newSampleTimestampTooNewError(metricName string, timestamp int64) ValidationError {
	return &sampleValidationError{
		message:    "timestamp too new: %d metric: %.200q",
		metricName: metricName,
		timestamp:  timestamp,
	}
}

// formatLabelSet formats label adapters as a metric name with labels, while preserving
// label order, and keeping duplicates. If there are multiple "__name__" labels, only
// first one is used as metric name, other ones will be included as regular labels.
func formatLabelSet(ls []cortexpb.LabelAdapter) string {
	metricName, hasMetricName := "", false

	labelStrings := make([]string, 0, len(ls))
	for _, l := range ls {
		if l.Name == model.MetricNameLabel && !hasMetricName && l.Value != "" {
			metricName = l.Value
			hasMetricName = true
		} else {
			labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", l.Name, l.Value))
		}
	}

	if len(labelStrings) == 0 {
		if hasMetricName {
			return metricName
		}
		return "{}"
	}

	return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ", "))
}
