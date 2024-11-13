package validation

import (
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
)

const (
	discardReasonLabel = "reason"

	errMetadataMissingMetricName = "metadata missing metric name"
	errMetadataTooLong           = "metadata '%s' value too long: %.200q metric %.200q"
	errMetadataV2TooLong         = "metadata '%s' value too long: %.200q"

	typeMetricName = "METRIC_NAME"
	typeHelp       = "HELP"
	typeUnit       = "UNIT"

	metricNameTooLong = "metric_name_too_long"
	helpTooLong       = "help_too_long"
	unitTooLong       = "unit_too_long"

	// ErrQueryTooLong is used in chunk store, querier and query frontend.
	ErrQueryTooLong = "the query time range exceeds the limit (query length: %s, limit: %s)"

	missingMetricName       = "missing_metric_name"
	invalidMetricName       = "metric_name_invalid"
	greaterThanMaxSampleAge = "greater_than_max_sample_age"
	maxLabelNamesPerSeries  = "max_label_names_per_series"
	tooFarInFuture          = "too_far_in_future"
	invalidLabel            = "label_invalid"
	labelNameTooLong        = "label_name_too_long"
	duplicateLabelNames     = "duplicate_label_names"
	labelsNotSorted         = "labels_not_sorted"
	labelValueTooLong       = "label_value_too_long"
	labelsSizeBytesExceeded = "labels_size_bytes_exceeded"

	// Exemplar-specific validation reasons
	exemplarLabelsMissing    = "exemplar_labels_missing"
	exemplarLabelsTooLong    = "exemplar_labels_too_long"
	exemplarTimestampInvalid = "exemplar_timestamp_invalid"

	// Native Histogram specific validation reasons
	nativeHistogramBucketCountLimitExceeded = "native_histogram_buckets_exceeded"

	// RateLimited is one of the values for the reason to discard samples.
	// Declared here to avoid duplication in ingester and distributor.
	RateLimited = "rate_limited"

	// Too many HA clusters is one of the reasons for discarding samples.
	TooManyHAClusters = "too_many_ha_clusters"

	// DroppedByRelabelConfiguration Samples can also be discarded because of relabeling configuration
	DroppedByRelabelConfiguration = "relabel_configuration"
	// DroppedByUserConfigurationOverride Samples discarded due to user configuration removing label __name__
	DroppedByUserConfigurationOverride = "user_label_removal_configuration"

	// The combined length of the label names and values of an Exemplar's LabelSet MUST NOT exceed 128 UTF-8 characters
	// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars
	ExemplarMaxLabelSetLength = 128
)

type ValidateMetrics struct {
	DiscardedSamples                  *prometheus.CounterVec
	DiscardedExemplars                *prometheus.CounterVec
	DiscardedMetadata                 *prometheus.CounterVec
	HistogramSamplesReducedResolution *prometheus.CounterVec
}

func registerCollector(r prometheus.Registerer, c prometheus.Collector) {
	err := r.Register(c)
	if err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
		panic(err)
	}
}

func NewValidateMetrics(r prometheus.Registerer) *ValidateMetrics {
	discardedSamples := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cortex_discarded_samples_total",
			Help: "The total number of samples that were discarded.",
		},
		[]string{discardReasonLabel, "user"},
	)
	registerCollector(r, discardedSamples)
	discardedExemplars := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cortex_discarded_exemplars_total",
			Help: "The total number of exemplars that were discarded.",
		},
		[]string{discardReasonLabel, "user"},
	)
	registerCollector(r, discardedExemplars)
	discardedMetadata := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cortex_discarded_metadata_total",
			Help: "The total number of metadata that were discarded.",
		},
		[]string{discardReasonLabel, "user"},
	)
	registerCollector(r, discardedMetadata)
	histogramSamplesReducedResolution := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cortex_reduced_resolution_histogram_samples_total",
			Help: "The total number of histogram samples that had the resolution reduced.",
		},
		[]string{"user"},
	)
	registerCollector(r, histogramSamplesReducedResolution)
	m := &ValidateMetrics{
		DiscardedSamples:                  discardedSamples,
		DiscardedExemplars:                discardedExemplars,
		DiscardedMetadata:                 discardedMetadata,
		HistogramSamplesReducedResolution: histogramSamplesReducedResolution,
	}

	return m
}

// ValidateSampleTimestamp returns an err if the sample timestamp is invalid.
// The returned error may retain the provided series labels.
func ValidateSampleTimestamp(validateMetrics *ValidateMetrics, limits *Limits, userID string, ls []cortexpb.LabelAdapter, timestampMs int64) ValidationError {
	unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)

	if limits.RejectOldSamples && model.Time(timestampMs) < model.Now().Add(-time.Duration(limits.RejectOldSamplesMaxAge)) {
		validateMetrics.DiscardedSamples.WithLabelValues(greaterThanMaxSampleAge, userID).Inc()
		return newSampleTimestampTooOldError(unsafeMetricName, timestampMs)
	}

	if model.Time(timestampMs) > model.Now().Add(time.Duration(limits.CreationGracePeriod)) {
		validateMetrics.DiscardedSamples.WithLabelValues(tooFarInFuture, userID).Inc()
		return newSampleTimestampTooNewError(unsafeMetricName, timestampMs)
	}

	return nil
}

// ValidateExemplarV2 returns an error if the exemplar is invalid.
// The returned error may retain the provided series labels.
func ValidateExemplarV2(validateMetrics *ValidateMetrics, symbols []string, userID string, seriesLabels []cortexpb.LabelAdapter, e *cortexpb.ExemplarV2, b labels.ScratchBuilder, st *writev2.SymbolsTable) ValidationError {
	lbs := e.ToLabels(&b, symbols)
	// symbolize examplar labels
	e.LabelsRefs = st.SymbolizeLabels(lbs, nil)
	exemplarLabels := cortexpb.FromLabelsToLabelAdapters(lbs)

	if len(exemplarLabels) <= 0 {
		validateMetrics.DiscardedExemplars.WithLabelValues(exemplarLabelsMissing, userID).Inc()
		return newExemplarEmtpyLabelsError(seriesLabels, []cortexpb.LabelAdapter{}, e.Timestamp)
	}

	if e.Timestamp == 0 {
		validateMetrics.DiscardedExemplars.WithLabelValues(exemplarTimestampInvalid, userID).Inc()
		return newExemplarMissingTimestampError(
			seriesLabels,
			exemplarLabels,
			e.Timestamp,
		)
	}

	// Exemplar label length does not include chars involved in text
	// rendering such as quotes, commas, etc.  See spec and const definition.
	labelSetLen := 0
	for _, l := range exemplarLabels {
		labelSetLen += utf8.RuneCountInString(l.Name)
		labelSetLen += utf8.RuneCountInString(l.Value)
	}

	if labelSetLen > ExemplarMaxLabelSetLength {
		validateMetrics.DiscardedExemplars.WithLabelValues(exemplarLabelsTooLong, userID).Inc()
		return newExemplarLabelLengthError(
			seriesLabels,
			exemplarLabels,
			e.Timestamp,
		)
	}

	return nil
}

// ValidateExemplar returns an error if the exemplar is invalid.
// The returned error may retain the provided series labels.
func ValidateExemplar(validateMetrics *ValidateMetrics, userID string, ls []cortexpb.LabelAdapter, e cortexpb.Exemplar) ValidationError {
	if len(e.Labels) <= 0 {
		validateMetrics.DiscardedExemplars.WithLabelValues(exemplarLabelsMissing, userID).Inc()
		return newExemplarEmtpyLabelsError(ls, []cortexpb.LabelAdapter{}, e.TimestampMs)
	}

	if e.TimestampMs == 0 {
		validateMetrics.DiscardedExemplars.WithLabelValues(exemplarTimestampInvalid, userID).Inc()
		return newExemplarMissingTimestampError(
			ls,
			e.Labels,
			e.TimestampMs,
		)
	}

	// Exemplar label length does not include chars involved in text
	// rendering such as quotes, commas, etc.  See spec and const definition.
	labelSetLen := 0
	for _, l := range e.Labels {
		labelSetLen += utf8.RuneCountInString(l.Name)
		labelSetLen += utf8.RuneCountInString(l.Value)
	}

	if labelSetLen > ExemplarMaxLabelSetLength {
		validateMetrics.DiscardedExemplars.WithLabelValues(exemplarLabelsTooLong, userID).Inc()
		return newExemplarLabelLengthError(
			ls,
			e.Labels,
			e.TimestampMs,
		)
	}

	return nil
}

// ValidateLabels returns an err if the labels are invalid.
// The returned error may retain the provided series labels.
func ValidateLabels(validateMetrics *ValidateMetrics, limits *Limits, userID string, ls []cortexpb.LabelAdapter, skipLabelNameValidation bool) ValidationError {
	if limits.EnforceMetricName {
		unsafeMetricName, err := extract.UnsafeMetricNameFromLabelAdapters(ls)
		if err != nil {
			validateMetrics.DiscardedSamples.WithLabelValues(missingMetricName, userID).Inc()
			return newNoMetricNameError()
		}

		if !model.IsValidMetricName(model.LabelValue(unsafeMetricName)) {
			validateMetrics.DiscardedSamples.WithLabelValues(invalidMetricName, userID).Inc()
			return newInvalidMetricNameError(unsafeMetricName)
		}
	}

	numLabelNames := len(ls)
	if numLabelNames > limits.MaxLabelNamesPerSeries {
		validateMetrics.DiscardedSamples.WithLabelValues(maxLabelNamesPerSeries, userID).Inc()
		return newTooManyLabelsError(ls, limits.MaxLabelNamesPerSeries)
	}

	maxLabelNameLength := limits.MaxLabelNameLength
	maxLabelValueLength := limits.MaxLabelValueLength
	lastLabelName := ""
	maxLabelsSizeBytes := limits.MaxLabelsSizeBytes
	labelsSizeBytes := 0

	for _, l := range ls {
		if !skipLabelNameValidation && !model.LabelName(l.Name).IsValid() {
			validateMetrics.DiscardedSamples.WithLabelValues(invalidLabel, userID).Inc()
			return newInvalidLabelError(ls, l.Name)
		} else if len(l.Name) > maxLabelNameLength {
			validateMetrics.DiscardedSamples.WithLabelValues(labelNameTooLong, userID).Inc()
			return newLabelNameTooLongError(ls, l.Name, maxLabelNameLength)
		} else if len(l.Value) > maxLabelValueLength {
			validateMetrics.DiscardedSamples.WithLabelValues(labelValueTooLong, userID).Inc()
			return newLabelValueTooLongError(ls, l.Name, l.Value, maxLabelValueLength)
		} else if cmp := strings.Compare(lastLabelName, l.Name); cmp >= 0 {
			if cmp == 0 {
				validateMetrics.DiscardedSamples.WithLabelValues(duplicateLabelNames, userID).Inc()
				return newDuplicatedLabelError(ls, l.Name)
			}

			validateMetrics.DiscardedSamples.WithLabelValues(labelsNotSorted, userID).Inc()
			return newLabelsNotSortedError(ls, l.Name)
		}

		lastLabelName = l.Name
		labelsSizeBytes += l.Size()
	}
	if maxLabelsSizeBytes > 0 && labelsSizeBytes > maxLabelsSizeBytes {
		validateMetrics.DiscardedSamples.WithLabelValues(labelsSizeBytesExceeded, userID).Inc()
		return labelSizeBytesExceededError(ls, labelsSizeBytes, maxLabelsSizeBytes)
	}
	return nil
}

// ValidateMetadata returns an err if a metric metadata is invalid.
func ValidateMetadataV2(validateMetrics *ValidateMetrics, cfg *Limits, userID string, symbols []string, metadata *cortexpb.MetadataV2, st *writev2.SymbolsTable) error {
	help := symbols[metadata.HelpRef]
	unit := symbols[metadata.UnitRef]

	// symbolize help and unit
	metadata.HelpRef = st.Symbolize(help)
	metadata.UnitRef = st.Symbolize(unit)

	maxMetadataValueLength := cfg.MaxMetadataLength
	var reason string
	var cause string
	var metadataType string
	if len(help) > maxMetadataValueLength {
		metadataType = typeHelp
		reason = helpTooLong
		cause = help
	} else if len(unit) > maxMetadataValueLength {
		metadataType = typeUnit
		reason = unitTooLong
		cause = unit
	}

	if reason != "" {
		validateMetrics.DiscardedMetadata.WithLabelValues(reason, userID).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errMetadataV2TooLong, metadataType, cause)
	}

	return nil
}

// ValidateMetadata returns an err if a metric metadata is invalid.
func ValidateMetadata(validateMetrics *ValidateMetrics, cfg *Limits, userID string, metadata *cortexpb.MetricMetadata) error {
	if cfg.EnforceMetadataMetricName && metadata.GetMetricFamilyName() == "" {
		validateMetrics.DiscardedMetadata.WithLabelValues(missingMetricName, userID).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errMetadataMissingMetricName)
	}

	maxMetadataValueLength := cfg.MaxMetadataLength
	var reason string
	var cause string
	var metadataType string
	if len(metadata.GetMetricFamilyName()) > maxMetadataValueLength {
		metadataType = typeMetricName
		reason = metricNameTooLong
		cause = metadata.GetMetricFamilyName()
	} else if len(metadata.Help) > maxMetadataValueLength {
		metadataType = typeHelp
		reason = helpTooLong
		cause = metadata.Help
	} else if len(metadata.Unit) > maxMetadataValueLength {
		metadataType = typeUnit
		reason = unitTooLong
		cause = metadata.Unit
	}

	if reason != "" {
		validateMetrics.DiscardedMetadata.WithLabelValues(reason, userID).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errMetadataTooLong, metadataType, cause, metadata.GetMetricFamilyName())
	}

	return nil
}

func ValidateNativeHistogram(validateMetrics *ValidateMetrics, limits *Limits, userID string, ls []cortexpb.LabelAdapter, histogramSample cortexpb.Histogram) (cortexpb.Histogram, error) {
	if limits.MaxNativeHistogramBuckets == 0 {
		return histogramSample, nil
	}

	var (
		exceedLimit bool
	)
	if histogramSample.IsFloatHistogram() {
		// Initial check to see if the bucket limit is exceeded or not. If not, we can avoid type casting.
		exceedLimit = len(histogramSample.PositiveCounts)+len(histogramSample.NegativeCounts) > limits.MaxNativeHistogramBuckets
		if !exceedLimit {
			return histogramSample, nil
		}
		// Exceed limit.
		if histogramSample.Schema <= histogram.ExponentialSchemaMin {
			validateMetrics.DiscardedSamples.WithLabelValues(nativeHistogramBucketCountLimitExceeded, userID).Inc()
			return cortexpb.Histogram{}, newHistogramBucketLimitExceededError(ls, limits.MaxNativeHistogramBuckets)
		}
		fh := cortexpb.FloatHistogramProtoToFloatHistogram(histogramSample)
		oBuckets := len(fh.PositiveBuckets) + len(fh.NegativeBuckets)
		for len(fh.PositiveBuckets)+len(fh.NegativeBuckets) > limits.MaxNativeHistogramBuckets {
			if fh.Schema <= histogram.ExponentialSchemaMin {
				validateMetrics.DiscardedSamples.WithLabelValues(nativeHistogramBucketCountLimitExceeded, userID).Inc()
				return cortexpb.Histogram{}, newHistogramBucketLimitExceededError(ls, limits.MaxNativeHistogramBuckets)
			}
			fh = fh.ReduceResolution(fh.Schema - 1)
		}
		if oBuckets != len(fh.PositiveBuckets)+len(fh.NegativeBuckets) {
			validateMetrics.HistogramSamplesReducedResolution.WithLabelValues(userID).Inc()
		}
		// If resolution reduced, convert new float histogram to protobuf type again.
		return cortexpb.FloatHistogramToHistogramProto(histogramSample.TimestampMs, fh), nil
	}

	// Initial check to see if bucket limit is exceeded or not. If not, we can avoid type casting.
	exceedLimit = len(histogramSample.PositiveDeltas)+len(histogramSample.NegativeDeltas) > limits.MaxNativeHistogramBuckets
	if !exceedLimit {
		return histogramSample, nil
	}
	// Exceed limit.
	if histogramSample.Schema <= histogram.ExponentialSchemaMin {
		validateMetrics.DiscardedSamples.WithLabelValues(nativeHistogramBucketCountLimitExceeded, userID).Inc()
		return cortexpb.Histogram{}, newHistogramBucketLimitExceededError(ls, limits.MaxNativeHistogramBuckets)
	}
	h := cortexpb.HistogramProtoToHistogram(histogramSample)
	oBuckets := len(h.PositiveBuckets) + len(h.NegativeBuckets)
	for len(h.PositiveBuckets)+len(h.NegativeBuckets) > limits.MaxNativeHistogramBuckets {
		if h.Schema <= histogram.ExponentialSchemaMin {
			validateMetrics.DiscardedSamples.WithLabelValues(nativeHistogramBucketCountLimitExceeded, userID).Inc()
			return cortexpb.Histogram{}, newHistogramBucketLimitExceededError(ls, limits.MaxNativeHistogramBuckets)
		}
		h = h.ReduceResolution(h.Schema - 1)
	}
	if oBuckets != len(h.PositiveBuckets)+len(h.NegativeBuckets) {
		validateMetrics.HistogramSamplesReducedResolution.WithLabelValues(userID).Inc()
	}
	// If resolution reduced, convert new histogram to protobuf type again.
	return cortexpb.HistogramToHistogramProto(histogramSample.TimestampMs, h), nil
}

func DeletePerUserValidationMetrics(validateMetrics *ValidateMetrics, userID string, log log.Logger) {
	filter := map[string]string{"user": userID}

	if err := util.DeleteMatchingLabels(validateMetrics.DiscardedSamples, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_discarded_samples_total metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(validateMetrics.DiscardedExemplars, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_discarded_exemplars_total metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(validateMetrics.DiscardedMetadata, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_discarded_metadata_total metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(validateMetrics.HistogramSamplesReducedResolution, filter); err != nil {
		level.Warn(log).Log("msg", "failed to remove cortex_reduced_resolution_histogram_samples_total metric for user", "user", userID, "err", err)
	}
}
