package ingester

import (
	"fmt"
	"net/http"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/httpgrpc"
)

type validationError struct {
	err       error // underlying error
	errorType string
	code      int
	noReport  bool // if true, error will be counted but not reported to caller
	labels    labels.Labels
}

func makeLimitError(errorType string, err error) error {
	return &validationError{
		errorType: errorType,
		err:       err,
		code:      http.StatusTooManyRequests,
	}
}

func makeNoReportError(errorType string) error {
	return &validationError{
		errorType: errorType,
		noReport:  true,
	}
}

func makeMetricValidationError(errorType string, labels labels.Labels, err error) error {
	return &validationError{
		errorType: errorType,
		err:       err,
		code:      http.StatusBadRequest,
		labels:    labels,
	}
}

func makeMetricLimitError(errorType string, labels labels.Labels, err error) error {
	return &validationError{
		errorType: errorType,
		err:       err,
		code:      http.StatusTooManyRequests,
		labels:    labels,
	}
}

func (e *validationError) WrapWithUser(userID string) *validationError {
	e.err = wrapWithUser(e.err, userID)
	return e
}

func (e *validationError) Error() string {
	if e.err == nil {
		return e.errorType
	}
	if e.labels == nil {
		return e.err.Error()
	}
	return fmt.Sprintf("%s for series %s", e.err.Error(), e.labels.String())
}

// WrappedError returns a HTTP gRPC error than is correctly forwarded over gRPC.
func (e *validationError) WrappedError() error {
	return httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: int32(e.code),
		Body: []byte(e.Error()),
	})
}

func wrapWithUser(err error, userID string) error {
	return errors.Wrapf(err, "user=%s", userID)
}
