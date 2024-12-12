package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// ValidationError is an error returned by series validation.
//
// Ignore stutter warning.
// nolint:revive
type ValidationError error

// genericValidationError is a basic implementation of ValidationError which can be used when the
// error format only contains the cause and the series.
type genericValidationError struct {
	message string
	cause   string
	series  []cortexpb.LabelPair
}

func (e *genericValidationError) Error() string {
	return fmt.Sprintf(e.message, e.cause, formatLabelSet(e.series))
}

// labelNameTooLongError is a customized ValidationError, in that the cause and the series are
// formatted in different order in Error.
type labelNameTooLongError struct {
	labelName string
	series    []cortexpb.LabelPair
	limit     int
}

func (e *labelNameTooLongError) Error() string {
	return fmt.Sprintf("label name too long for metric (actual: %d, limit: %d) metric: %.200q label name: %.200q", len(e.labelName), e.limit, formatLabelSet(e.series), e.labelName)
}

func newLabelNameTooLongError(series []cortexpb.LabelPair, labelName string, limit int) ValidationError {
	return &labelNameTooLongError{
		labelName: labelName,
		series:    series,
		limit:     limit,
	}
}

// labelValueTooLongError is a customized ValidationError, in that the cause and the series are
// formatted in different order in Error.
type labelValueTooLongError struct {
	labelName  string
	labelValue string
	series     []cortexpb.LabelPair
	limit      int
}

func (e *labelValueTooLongError) Error() string {
	return fmt.Sprintf("label value too long for metric (actual: %d, limit: %d) metric: %.200q label name: %.200q label value: %.200q",
		len(e.labelValue), e.limit, formatLabelSet(e.series), e.labelName, e.labelValue)
}

func newLabelValueTooLongError(series []cortexpb.LabelPair, labelName, labelValue string, limit int) ValidationError {
	return &labelValueTooLongError{
		labelName:  labelName,
		labelValue: labelValue,
		series:     series,
		limit:      limit,
	}
}

// labelsSizeBytesExceededError is a customized ValidationError, in that the cause and the series are
// formatted in different order in Error.
type labelsSizeBytesExceededError struct {
	labelsSizeBytes int
	series          []cortexpb.LabelPair
	limit           int
}

func (e *labelsSizeBytesExceededError) Error() string {
	return fmt.Sprintf("labels size bytes exceeded for metric (actual: %d, limit: %d) metric: %.200q", e.labelsSizeBytes, e.limit, formatLabelSet(e.series))
}

func labelSizeBytesExceededError(series []cortexpb.LabelPair, labelsSizeBytes int, limit int) ValidationError {
	return &labelsSizeBytesExceededError{
		labelsSizeBytes: labelsSizeBytes,
		series:          series,
		limit:           limit,
	}
}

func newInvalidLabelError(series []cortexpb.LabelPair, labelName string) ValidationError {
	return &genericValidationError{
		message: "sample invalid label: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newDuplicatedLabelError(series []cortexpb.LabelPair, labelName string) ValidationError {
	return &genericValidationError{
		message: "duplicate label name: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

func newLabelsNotSortedError(series []cortexpb.LabelPair, labelName string) ValidationError {
	return &genericValidationError{
		message: "labels not sorted: %.200q metric %.200q",
		cause:   labelName,
		series:  series,
	}
}

type tooManyLabelsError struct {
	series []cortexpb.LabelPair
	limit  int
}

func newTooManyLabelsError(series []cortexpb.LabelPair, limit int) ValidationError {
	return &tooManyLabelsError{
		series: series,
		limit:  limit,
	}
}

func (e *tooManyLabelsError) Error() string {
	return fmt.Sprintf(
		"series has too many labels (actual: %d, limit: %d) series: '%s'",
		len(e.series), e.limit, cortexpb.FromLabelAdaptersToMetric(e.series).String())
}

type noMetricNameError struct{}

func newNoMetricNameError() ValidationError {
	return &noMetricNameError{}
}

func (e *noMetricNameError) Error() string {
	return "sample missing metric name"
}

type invalidMetricNameError struct {
	metricName string
}

func newInvalidMetricNameError(metricName string) ValidationError {
	return &invalidMetricNameError{
		metricName: metricName,
	}
}

func (e *invalidMetricNameError) Error() string {
	return fmt.Sprintf("sample invalid metric name: %.200q", e.metricName)
}

// sampleValidationError is a ValidationError implementation suitable for sample validation errors.
type sampleValidationError struct {
	message    string
	metricName string
	timestamp  int64
}

func (e *sampleValidationError) Error() string {
	return fmt.Sprintf(e.message, e.timestamp, e.metricName)
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

// exemplarValidationError is a ValidationError implementation suitable for exemplar validation errors.
type exemplarValidationError struct {
	message        string
	seriesLabels   []cortexpb.LabelPair
	exemplarLabels []cortexpb.LabelPair
	timestamp      int64
}

func (e *exemplarValidationError) Error() string {
	return fmt.Sprintf(e.message, e.timestamp, cortexpb.FromLabelAdaptersToLabels(e.seriesLabels).String(), cortexpb.FromLabelAdaptersToLabels(e.exemplarLabels).String())
}

func newExemplarEmtpyLabelsError(seriesLabels []cortexpb.LabelPair, exemplarLabels []cortexpb.LabelPair, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        "exemplar missing labels, timestamp: %d series: %s labels: %s",
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

func newExemplarMissingTimestampError(seriesLabels []cortexpb.LabelPair, exemplarLabels []cortexpb.LabelPair, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        "exemplar missing timestamp, timestamp: %d series: %s labels: %s",
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

var labelLenMsg = "exemplar combined labelset exceeds " + strconv.Itoa(ExemplarMaxLabelSetLength) + " characters, timestamp: %d series: %s labels: %s"

func newExemplarLabelLengthError(seriesLabels []cortexpb.LabelPair, exemplarLabels []cortexpb.LabelPair, timestamp int64) ValidationError {
	return &exemplarValidationError{
		message:        labelLenMsg,
		seriesLabels:   seriesLabels,
		exemplarLabels: exemplarLabels,
		timestamp:      timestamp,
	}
}

// histogramBucketLimitExceededError is a ValidationError implementation for samples with native histogram
// exceeding max bucket limit and cannot reduce resolution further to be within the max bucket limit.
type histogramBucketLimitExceededError struct {
	series []cortexpb.LabelPair
	limit  int
}

func newHistogramBucketLimitExceededError(series []cortexpb.LabelPair, limit int) ValidationError {
	return &histogramBucketLimitExceededError{
		series: series,
		limit:  limit,
	}
}

func (e *histogramBucketLimitExceededError) Error() string {
	return fmt.Sprintf("native histogram bucket count exceeded for metric (limit: %d) metric: %.200q", e.limit, formatLabelSet(e.series))
}

// formatLabelSet formats label adapters as a metric name with labels, while preserving
// label order, and keeping duplicates. If there are multiple "__name__" labels, only
// first one is used as metric name, other ones will be included as regular labels.
func formatLabelSet(ls []cortexpb.LabelPair) string {
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
