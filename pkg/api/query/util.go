package query

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"
)

const (
	statusSuccess = "success"
	statusError   = "error"

	// Non-standard status code (originally introduced by nginx) for the case when a client closes
	// the connection while the server is still processing the request.
	statusClientClosedConnection = 499
)

type errorType string

const (
	errorTimeout       errorType = "timeout"
	errorCanceled      errorType = "canceled"
	errorExec          errorType = "execution"
	errorBadData       errorType = "bad_data"
	errorInternal      errorType = "internal"
	errorNotFound      errorType = "not_found"
	errorNotAcceptable errorType = "not_acceptable"
)

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

func returnAPIError(err error) *apiError {
	if err == nil {
		return nil
	}

	var eqc promql.ErrQueryCanceled
	var eqt promql.ErrQueryTimeout
	var es promql.ErrStorage

	switch {
	case errors.As(err, &eqc):
		return &apiError{errorCanceled, err}
	case errors.As(err, &eqt):
		return &apiError{errorTimeout, err}
	case errors.As(err, &es):
		return &apiError{errorInternal, err}
	}

	if errors.Is(err, context.Canceled) {
		return &apiError{errorCanceled, err}
	}

	return &apiError{errorExec, err}
}

type apiFuncResult struct {
	data      interface{}
	err       *apiError
	warnings  annotations.Annotations
	finalizer func()
}

type apiFunc func(r *http.Request) apiFuncResult

func invalidParamError(err error, parameter string) apiFuncResult {
	return apiFuncResult{nil, &apiError{
		errorBadData, fmt.Errorf("invalid parameter %q: %w", parameter, err),
	}, nil, nil}
}
