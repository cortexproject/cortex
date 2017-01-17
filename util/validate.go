package util

import (
	"regexp"

	"github.com/prometheus/common/model"
)

// See https://dave.cheney.net/2016/04/07/constant-errors
type Error string

func (e Error) Error() string { return string(e) }

// Errors returned by ValidateSample
const (
	ErrMissingMetricName = Error("sample missing metric name")
	ErrInvalidMetricName = Error("sample invalid metric name")
	ErrInvalidLabel      = Error("sample invalid label")
)

var (
	validLabelRE      = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	validMetricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
)

// ValidateSample returns an err if the sample is invalid
func ValidateSample(s *model.Sample) error {
	metricName, ok := s.Metric[model.MetricNameLabel]
	if !ok {
		return ErrMissingMetricName
	}

	if !validMetricNameRE.MatchString(string(metricName)) {
		return ErrInvalidMetricName
	}

	for k := range s.Metric {
		if !validLabelRE.MatchString(string(k)) {
			return ErrInvalidLabel
		}
	}
	return nil
}
