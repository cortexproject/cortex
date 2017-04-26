package util

import (
	"regexp"

	"github.com/prometheus/common/model"
)

var (
	validLabelRE        = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	validMetricNameRE   = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	maxLabelNameLength  = 1024
	maxLabelValueLength = 4096
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

	for k, v := range s.Metric {
		if !validLabelRE.MatchString(string(k)) {
			return ErrInvalidLabel
		}
		if len(k) > maxLabelNameLength {
			return ErrLabelNameTooLong
		}
		if len(v) > maxLabelValueLength {
			return ErrLabelValueTooLong
		}
	}
	return nil
}
