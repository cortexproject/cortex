package ingester

import (
	"fmt"
	"net/http"

	"github.com/prometheus/prometheus/model/labels"
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
	if e.labels == nil {
		return e.err.Error()
	}
	return fmt.Sprintf("%s for series %s", e.err.Error(), e.labels.String())
}

// wrapWithUser prepends the user to the error. It does not retain a reference to err.
func wrapWithUser(err error, userID string) error {
	return fmt.Errorf("user=%s: %s", userID, err)
}
