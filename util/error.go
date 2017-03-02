package util

import "github.com/weaveworks/common/error"

// Errors returned by Cortex components.
const (
	ErrMissingMetricName         = error.Error("sample missing metric name")
	ErrInvalidMetricName         = error.Error("sample invalid metric name")
	ErrInvalidLabel              = error.Error("sample invalid label")
	ErrUserSeriesLimitExceeded   = error.Error("per-user series limit exceeded")
	ErrMetricSeriesLimitExceeded = error.Error("per-metric series limit exceeded")
)
