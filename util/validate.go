package util

import (
	"regexp"

	"github.com/prometheus/common/model"
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
