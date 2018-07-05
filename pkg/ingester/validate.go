package ingester

import (
	"net/http"
	"time"

	"flag"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"
)

const (
	errMissingMetricName = "sample missing metric name"
	errInvalidMetricName = "sample invalid metric name: %.200q"
	errInvalidLabel      = "sample invalid label: %.200q metric %.200q"
	errLabelNameTooLong  = "label name too long: %.200q metric %.200q"
	errLabelValueTooLong = "label value too long: %.200q metric %.200q"
	errTooManyLabels     = "sample for '%s' has %d label names; limit %d"
	errTooOld            = "sample for '%s' has timestamp too old: %v"
	errTooNew            = "sample for '%s' has timestamp too new: %v"
)

// ValidateConfig has config for validation settings and options
type ValidateConfig struct {
	// maximum length a label name can be
	MaxLabelNameLength int
	// maximum length a label value can be. This also is the maximum length of a metric name.
	MaxLabelValueLength int
	// maximum number of label/value pairs timeseries.
	MaxLabelNamesPerSeries int
	// Config for rejecting old samples.
	RejectOldSamples       bool
	RejectOldSamplesMaxAge time.Duration
	CreationGracePeriod    time.Duration
}

// RegisterFlags registers a set of command line flags for setting options regarding sample validation at ingestion time.
func (cfg *ValidateConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxLabelNameLength, "ingester.validation.max-length-label-name", 1024, "Maximum length accepted for label names")
	f.IntVar(&cfg.MaxLabelValueLength, "ingester.validation.max-length-label-value", 2048, "Maximum length accepted for label value. This setting also applies to the metric name")
	f.IntVar(&cfg.MaxLabelNamesPerSeries, "ingester.max-label-names-per-series", 20, "Maximum number of label names per series.")
	f.BoolVar(&cfg.RejectOldSamples, "ingester.reject-old-samples", false, "Reject old samples.")
	f.DurationVar(&cfg.RejectOldSamplesMaxAge, "ingester.reject-old-samples.max-age", 14*24*time.Hour, "Maximum accepted sample age before rejecting.")
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

	if config.RejectOldSamples && s.Timestamp < model.Now().Add(-config.RejectOldSamplesMaxAge) {
		discardedSamples.WithLabelValues(greaterThanMaxSampleAge).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errTooOld, metricName, s.Timestamp)
	}
	if s.Timestamp > model.Now().Add(config.CreationGracePeriod) {
		discardedSamples.WithLabelValues(tooFarInFuture).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errTooNew, metricName, s.Timestamp)
	}

	numLabelNames := len(s.Metric)
	if numLabelNames > config.MaxLabelNamesPerSeries {
		discardedSamples.WithLabelValues(maxLabelNamesPerSeries).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errTooManyLabels, metricName, numLabelNames, config.MaxLabelNamesPerSeries)
	}

	for k, v := range s.Metric {
		if !k.IsValid() {
			return httpgrpc.Errorf(http.StatusBadRequest, errInvalidLabel, k, metricName)
		}
		if len(k) > config.MaxLabelNameLength {
			return httpgrpc.Errorf(http.StatusBadRequest, errLabelNameTooLong, k, metricName)
		}
		if len(v) > config.MaxLabelValueLength {
			return httpgrpc.Errorf(http.StatusBadRequest, errLabelValueTooLong, v, metricName)
		}
	}
	return nil
}
