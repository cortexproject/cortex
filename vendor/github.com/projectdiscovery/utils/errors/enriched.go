package errorutil

import (
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
)

// ShowStackTrace in Error Message
var ShowStackTrace bool = false

// ErrCallback function to handle given error
type ErrCallback func(level ErrorLevel, err string, tags ...string)

// enrichedError is enriched version of normal error
// with tags, stacktrace and other methods
type enrichedError struct {
	errString  string
	wrappedErr error
	StackTrace string
	Tags       []string
	Level      ErrorLevel

	//OnError is called when Error() method is triggered
	OnError ErrCallback
}

// withTag assignes tag to Error
func (e *enrichedError) WithTag(tag ...string) Error {
	if e.Tags == nil {
		e.Tags = tag
	} else {
		e.Tags = append(e.Tags, tag...)
	}
	return e
}

// withLevel assinges level to Error
func (e *enrichedError) WithLevel(level ErrorLevel) Error {
	e.Level = level
	return e
}

// Unwrap returns the underlying error
func (e *enrichedError) Unwrap() error {
	return e.wrappedErr
}

// returns formated *enrichedError string
func (e *enrichedError) Error() string {
	defer func() {
		if e.OnError != nil {
			e.OnError(e.Level, e.errString, e.Tags...)
		}
	}()
	var buff bytes.Buffer
	label := fmt.Sprintf("[%v:%v]", strings.Join(e.Tags, ","), e.Level.String())
	buff.WriteString(fmt.Sprintf("%v %v", label, e.errString))

	if ShowStackTrace {
		e.captureStack()
		buff.WriteString(fmt.Sprintf("Stacktrace:\n%v", e.StackTrace))
	}
	return buff.String()
}

// wraps given error
func (e *enrichedError) Wrap(err ...error) Error {
	for _, v := range err {
		if v == nil {
			continue
		}

		if e.wrappedErr == nil {
			e.wrappedErr = v
		} else {
			// wraps the existing wrapped error (maintains the error chain)
			e.wrappedErr = &enrichedError{
				errString:  v.Error(),
				wrappedErr: e.wrappedErr,
				Level:      e.Level,
			}
		}

		// preserve its props if it's an enriched one
		if ee, ok := v.(*enrichedError); ok {
			if len(ee.Tags) > 0 {
				if e.Tags == nil {
					e.Tags = make([]string, 0)
				}
				e.Tags = append(e.Tags, ee.Tags...)
			}

			if ee.StackTrace != "" {
				e.StackTrace += ee.StackTrace
			}
		}
	}

	return e
}

// Wrapf wraps given message
func (e *enrichedError) Msgf(format string, args ...any) Error {
	// wraps with '<-` as delimeter
	msg := fmt.Sprintf(format, args...)
	if e.errString == "" {
		e.errString = msg
	} else {
		e.errString = fmt.Sprintf("%v <- %v", msg, e.errString)
	}
	return e
}

// Equal returns true if error matches anyone of given errors
func (e *enrichedError) Equal(err ...error) bool {
	for _, v := range err {
		if ee, ok := v.(*enrichedError); ok {
			if e.errString == ee.errString {
				return true
			}
		} else {
			// not an enriched error but a simple error
			if e.errString == v.Error() {
				return true
			}
		}

		// also check if the err is in the wrapped chain
		if errors.Is(e, v) {
			return true
		}
	}

	return false
}

// WithCallback executes callback when error is triggered
func (e *enrichedError) WithCallback(handle ErrCallback) Error {
	e.OnError = handle
	return e
}

// captureStack
func (e *enrichedError) captureStack() {
	// can be furthur improved to format
	// ref https://github.com/go-errors/errors/blob/33d496f939bc762321a636d4035e15c302eb0b00/stackframe.go
	e.StackTrace = string(debug.Stack())
}

// New
func New(format string, args ...any) Error {
	ee := &enrichedError{
		errString: fmt.Sprintf(format, args...),
		Level:     Runtime,
	}
	return ee
}

func NewWithErr(err error) Error {
	if err == nil {
		return nil
	}

	if ee, ok := err.(*enrichedError); ok {
		return &enrichedError{
			errString:  ee.errString,
			wrappedErr: err,
			StackTrace: ee.StackTrace,
			Tags:       append([]string{}, ee.Tags...),
			Level:      ee.Level,
			OnError:    ee.OnError,
		}
	}

	return &enrichedError{
		errString:  err.Error(),
		wrappedErr: err,
		Level:      Runtime,
	}
}

// NewWithTag creates an error with tag
func NewWithTag(tag string, format string, args ...any) Error {
	ee := New(format, args...)
	_ = ee.WithTag(tag)
	return ee
}
