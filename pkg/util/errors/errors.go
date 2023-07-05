package errors

import "errors"

type errWithCause struct {
	error
	cause error
}

func (e errWithCause) Error() string {
	return e.error.Error()
}

// Cause To support errors.Cause().
func (e errWithCause) Cause() error {
	return e.cause
}

// Is To support errors.Is().
func (e errWithCause) Is(err error) bool {
	return errors.Is(err, e.error) || errors.Is(err, e.cause)
}

// Unwrap To support errors.Unwrap().
func (e errWithCause) Unwrap() error {
	return e.cause
}

// Err return the original error
func (e errWithCause) Err() error {
	return e.error
}

// WithCause wrappers err with a error cause
func WithCause(err, cause error) error {
	return errWithCause{
		error: err,
		cause: cause,
	}
}

// ErrorIs is similar to `errors.Is` but receives a function to compare
func ErrorIs(err error, f func(err error) bool) bool {
	for {
		if f(err) {
			return true
		}
		if err = errors.Unwrap(err); err == nil {
			return false
		}
	}
}
