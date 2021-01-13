package flux

import (
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

type Error = errors.Error

// ErrorCode returns the error code for the given error.
// If the error is not a flux.Error, this will return
// Unknown for the code. If the error is a flux.Error
// and its code is Inherit, then this will return the
// wrapped error's code.
func ErrorCode(err error) codes.Code {
	return errors.Code(err)
}

// ErrorDocURL returns the DocURL associated with this error
// if one exists. This will return the outermost DocURL
// associated with this error unless the code is Inherit.
// If the code for an error is Inherit, this will return
// the DocURL for the nested error if it exists.
func ErrorDocURL(err error) string {
	return errors.DocURL(err)
}
