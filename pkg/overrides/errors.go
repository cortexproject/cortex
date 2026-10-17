package overrides

import "errors"

var (
	// ErrNotFound is returned when the requested overrides do not exist.
	ErrNotFound = errors.New("overrides not found")

	// ErrAccessDenied is returned when access to overrides is denied.
	ErrAccessDenied = errors.New("access denied")

	// ErrInvalidOverrides is returned when the provided overrides are invalid.
	ErrInvalidOverrides = errors.New("invalid overrides")
)
