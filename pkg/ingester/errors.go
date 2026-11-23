package ingester

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/status"
)

type validationError struct {
	err       error // underlying error
	errorType string
	code      int
	labels    labels.Labels
}

func makeLimitError(errorType string, err error) error {
	return &validationError{
		errorType: errorType,
		err:       err,
		code:      http.StatusBadRequest,
	}
}

func makeMetricLimitError(errorType string, labels labels.Labels, err error) error {
	return &validationError{
		errorType: errorType,
		err:       err,
		code:      http.StatusBadRequest,
		labels:    labels,
	}
}

func (e *validationError) Error() string {
	if e.err == nil {
		return e.errorType
	}
	if e.labels.IsEmpty() {
		return e.err.Error()
	}
	return fmt.Sprintf("%s for series %s", e.err.Error(), e.labels.String())
}

// wrapWithUser prepends the user to the error. It does not retain a reference to err.
func wrapWithUser(err error, userID string) error {
	return fmt.Errorf("user=%s: %s", userID, err)
}

// unloggedError wraps an error that is returned to the distributor but that the gRPC server should
// not log. It keeps the gRPC status of the wrapped error, so the response on the wire is unchanged.
type unloggedError struct {
	err error
}

func (e unloggedError) Error() string { return e.err.Error() }

func (e unloggedError) Unwrap() error { return e.err }

func (e unloggedError) GRPCStatus() *status.Status { return status.Convert(e.err) }

// ShouldLog implements middleware.OptionalLogging.
func (unloggedError) ShouldLog(context.Context, time.Duration) bool { return false }
