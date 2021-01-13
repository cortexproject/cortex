package errors

import (
	"fmt"
	"strings"

	"github.com/influxdata/flux/codes"
)

// Error is the error struct of flux.
type Error struct {
	// Code is the code of the error as defined in the codes package.
	// This describes the type and category of the error. It is required.
	Code codes.Code

	// Msg contains a human-readable description and additional information
	// about the error itself. This is optional.
	Msg string

	// DocURL contains a link to documentation that is related to explaining
	// details and/or solutions to this error message.
	DocURL string

	// Err contains the error that was the cause of this error.
	// This is optional.
	Err error
}

// Error implement the error interface by outputting the Code and Err.
func (e *Error) Error() string {
	if e.Msg != "" && e.Err != nil {
		var b strings.Builder
		b.WriteString(e.Msg)
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
		return b.String()
	} else if e.Msg != "" {
		return e.Msg
	} else if e.Err != nil {
		return e.Err.Error()
	}
	return e.Code.String()
}

// Unwrap will return the wrapped error.
func (e *Error) Unwrap() error {
	return e.Err
}

func New(code codes.Code, msg ...interface{}) *Error {
	return Wrap(nil, code, msg...)
}

func Newf(code codes.Code, fmtStr string, args ...interface{}) *Error {
	return Wrapf(nil, code, fmtStr, args...)
}

func Wrap(err error, code codes.Code, msg ...interface{}) *Error {
	var s string
	if len(msg) > 0 {
		s = fmt.Sprint(msg...)
	}
	return &Error{
		Code: code,
		Msg:  s,
		Err:  err,
	}
}

func Wrapf(err error, code codes.Code, format string, a ...interface{}) *Error {
	return &Error{
		Code: code,
		Msg:  fmt.Sprintf(format, a...),
		Err:  err,
	}
}

// Code returns the error code for the given error.
// If the error is not a flux.Error, this will return
// Unknown for the code. If the error is a flux.Error
// and its code is Inherit, then this will return the
// wrapped error's code.
func Code(err error) codes.Code {
	for {
		if ferr, ok := err.(*Error); ok {
			if ferr.Code != codes.Inherit {
				return ferr.Code
			} else if ferr.Err == nil {
				return codes.Unknown
			}
			err = ferr.Err
		} else {
			return codes.Unknown
		}
	}
}

// DocURL returns the DocURL associated with this error
// if one exists. This will return the outermost DocURL
// associated with this error unless the code is Inherit.
// If the code for an error is Inherit, this will return
// the DocURL for the nested error if it exists.
func DocURL(err error) string {
	for {
		if ferr, ok := err.(*Error); ok {
			if ferr.DocURL != "" {
				docURL := ferr.DocURL
				if ferr.Code == codes.Inherit && ferr.Err != nil {
					if nestedDocURL := DocURL(ferr.Err); nestedDocURL != "" {
						docURL = nestedDocURL
					}
				}
				return docURL
			} else if ferr.Err == nil {
				return ""
			}
			err = ferr.Err
		} else {
			return ""
		}
	}
}

// WithDocURL will annotate an error with a DocURL.
// If the error is an Error and the DocURL is not filled,
// it will be set. If the error is not an Error or the DocURL
// is filled, it will wrap the error and set the DocURL
// on the wrapper error.
func WithDocURL(err error, docURL string) *Error {
	if e, ok := err.(*Error); ok && e.DocURL == "" {
		e.DocURL = docURL
		return e
	}
	return &Error{
		Code:   codes.Inherit,
		DocURL: docURL,
		Err:    err,
	}
}

// WithDocURL can be used to add a documentation URL to the error.
func (e *Error) WithDocURL(docURL string) *Error {
	e.DocURL = docURL
	return e
}
