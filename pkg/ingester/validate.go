package ingester

import (
	"net/http"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"
)

const (
	errMissingMetricName = "sample missing metric name"
	errInvalidMetricName = "sample invalid metric name: '%s'"
	errInvalidLabel      = "sample invalid label: '%s'"
	errLabelNameTooLong  = "label name too long: '%s'"
	errLabelValueTooLong = "label value too long: '%s'"
)

// ValidateSample returns an err if the sample is invalid
func ValidateSample(s *model.Sample, maxLabelNameLength, maxLabelValueLength int) error {
	metricName, ok := s.Metric[model.MetricNameLabel]
	if !ok {
		return httpgrpc.Errorf(http.StatusBadRequest, errMissingMetricName)
	}

	if !model.IsValidMetricName(metricName) {
		return httpgrpc.Errorf(http.StatusBadRequest, errInvalidMetricName, metricName)
	}

	for k, v := range s.Metric {
		if !k.IsValid() {
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
