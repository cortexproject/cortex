package ingester

import (
	"net/http"

	"flag"

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

// Config for validation settings and options
type ValidateConfig struct {
	// maximum length a label name can be
	MaxLabelNameLength int
	// maximum length a label value can be. This also is the maximum length of a metric name.
	MaxLabelValueLength int
	// maximum number of label/value pairs timeseries.
	MaxLabelNamesPerSeries int
}

// RegisterFlags registers a set of command line flags for setting options regarding sample validation at ingestion time.
func (cfg *ValidateConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxLabelNameLength, "ingester.validation.max-length-label-name", 1024, "Maximum length accepted for label names")
	f.IntVar(&cfg.MaxLabelValueLength, "ingester.validation.max-length-label-value", 2048, "Maximum length accepted for label value. This setting also applies to the metric name")
	f.IntVar(&cfg.MaxLabelNamesPerSeries, "ingester.max-label-names-per-series", DefaultMaxLabelNamesPerSeries, "Maximum number of label names per series.")
}

// ValidateSample returns an err if the sample is invalid
func ValidateSample(s *model.Sample, config *ValidateConfig) error {
	metricName, ok := s.Metric[model.MetricNameLabel]
	if !ok {
		return httpgrpc.Errorf(http.StatusBadRequest, errMissingMetricName)
	}

	if !model.IsValidMetricName(metricName) {
		return httpgrpc.Errorf(http.StatusBadRequest, errInvalidMetricName, metricName)
	}

	numLabelNames := len(s.Metric)
	if numLabelNames > config.MaxLabelNamesPerSeries {
		discardedSamples.WithLabelValues(maxLabelNamesPerSeries).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, "sample has %d label names which is greater than the accepted maximum of %d", numLabelNames, config.MaxLabelNamesPerSeries)
	}

	for k, v := range s.Metric {
		if !k.IsValid() {
			return httpgrpc.Errorf(http.StatusBadRequest, errInvalidLabel, k)
		}
		if len(k) > config.MaxLabelNameLength {
			return httpgrpc.Errorf(http.StatusBadRequest, errLabelNameTooLong, k)
		}
		if len(v) > config.MaxLabelValueLength {
			return httpgrpc.Errorf(http.StatusBadRequest, errLabelValueTooLong, v)
		}
	}
	return nil
}
