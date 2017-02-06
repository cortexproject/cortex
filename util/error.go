package util

// Error see https://dave.cheney.net/2016/04/07/constant-errors.
type Error string

func (e Error) Error() string { return string(e) }

// Errors returned by Cortex components.
const (
	ErrMissingMetricName       = Error("sample missing metric name")
	ErrInvalidMetricName       = Error("sample invalid metric name")
	ErrInvalidLabel            = Error("sample invalid label")
	ErrUserSeriesLimitExceeded = Error("per-user series limit exceeded")
)
