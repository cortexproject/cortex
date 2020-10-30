package util

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrStopProcess is the error returned by a service as a hint to stop the server entirely.
var ErrStopProcess = errors.New("stop process")

// MultiError type allows combining multiple errors into one.
type MultiError []error

// NewMultiError returns MultiError with provided errors added if not nil.
func NewMultiError(errs ...error) MultiError { // nolint:golint
	m := MultiError{}
	m.Add(errs...)
	return m
}

// Add adds single or many errors to the error list. Each error is added only if not nil.
// If the error is a nonNilMultiError type, the errors inside nonNilMultiError are added to the main MultiError.
func (es *MultiError) Add(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		}
		if merr, ok := err.(nonNilMultiError); ok {
			*es = append(*es, merr.errs...)
			continue
		}
		*es = append(*es, err)
	}
}

// Err returns the error list as an error or nil if it is empty.
func (es MultiError) Err() error {
	if len(es) == 0 {
		return nil
	}
	return nonNilMultiError{errs: es}
}

// nonNilMultiError implements the error interface, and it represents
// MultiError with at least one error inside it.
// This type is needed to make sure that nil is returned when no error is combined in MultiError for err != nil
// check to work.
type nonNilMultiError struct {
	errs MultiError
}

// Error returns a concatenated string of the contained errors.
func (es nonNilMultiError) Error() string {
	var buf bytes.Buffer

	if len(es.errs) > 1 {
		fmt.Fprintf(&buf, "%d errors: ", len(es.errs))
	}

	for i, err := range es.errs {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}

	return buf.String()
}
