package util

import "github.com/weaveworks/common/errors"

// Errors returned by Cortex components.
const (
	ErrMissingMetricName         = errors.Error("sample missing metric name")
	ErrInvalidMetricName         = errors.Error("sample invalid metric name")
	ErrInvalidLabel              = errors.Error("sample invalid label")
	ErrUserSeriesLimitExceeded   = errors.Error("per-user series limit exceeded")
	ErrMetricSeriesLimitExceeded = errors.Error("per-metric series limit exceeded")
)
