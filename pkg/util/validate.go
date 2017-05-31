package util

import (
	"net/http"
	"regexp"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"
)

var (
	validLabelRE        = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	validMetricNameRE   = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	maxLabelNameLength  = 1024
	maxLabelValueLength = 4096
)

const (
	errMissingMetricName = "sample missing metric name"
	errInvalidMetricName = "sample invalid metric name: '%s'"
	errInvalidLabel      = "sample invalid label: '%s'"
	errLabelNameTooLong  = "label name too long: '%s'"
	errLabelValueTooLong = "label value too long: '%s'"
)

// ValidateSample returns an err if the sample is invalid
func ValidateSample(s *model.Sample) error {
	metricName, ok := s.Metric[model.MetricNameLabel]
	if !ok {
		return httpgrpc.Errorf(http.StatusBadRequest, errMissingMetricName)
	}

	if !validMetricNameRE.MatchString(string(metricName)) {
		return httpgrpc.Errorf(http.StatusBadRequest, errInvalidMetricName, metricName)
	}

	for k, v := range s.Metric {
		if !validLabelRE.MatchString(string(k)) {
			return httpgrpc.Errorf(http.StatusBadRequest, errInvalidLabel, k)
		}
		if len(k) > maxLabelNameLength {
			return httpgrpc.Errorf(http.StatusBadRequest, errLabelNameTooLong, k)
		}
		if len(v) > maxLabelValueLength {
			return httpgrpc.Errorf(http.StatusBadRequest, errLabelValueTooLong, v)
		}
	}
	return nil
}
