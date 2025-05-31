package queryapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gogo/status"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	ErrEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "%s", "end timestamp must not be before start time")
	ErrNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "%s", "zero or negative query resolution step widths are not accepted. Try a positive integer")
	ErrStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "%s", "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
)

func extractQueryOpts(r *http.Request) (promql.QueryOpts, error) {
	var duration time.Duration

	if strDuration := r.FormValue("lookback_delta"); strDuration != "" {
		parsedDuration, err := util.ParseDurationMs(strDuration)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, "error parsing lookback delta duration: %v", err)
		}
		duration = convertMsToDuration(parsedDuration)
	}

	return promql.NewPrometheusQueryOpts(r.FormValue("stats") == "all", duration), nil
}

const (
	statusSuccess = "success"

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
		return &apiError{errorCanceled, httpgrpc.Errorf(statusClientClosedConnection, "%v", err)}
	case errors.As(err, &eqt):
		return &apiError{errorTimeout, httpgrpc.Errorf(http.StatusServiceUnavailable, "%v", err)}
	case errors.As(err, &es):
		return &apiError{errorInternal, httpgrpc.Errorf(http.StatusInternalServerError, "%v", err)}
	}

	if errors.Is(err, context.Canceled) {
		return &apiError{errorCanceled, httpgrpc.Errorf(statusClientClosedConnection, "%v", err)}
	}

	return &apiError{errorExec, httpgrpc.Errorf(http.StatusUnprocessableEntity, "%v", err)}
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
		errorBadData, DecorateWithParamName(err, parameter),
	}, nil, nil}
}

func convertMsToTime(unixMs int64) time.Time {
	return time.Unix(0, unixMs*int64(time.Millisecond))
}

func convertMsToDuration(unixMs int64) time.Duration {
	return time.Duration(unixMs) * time.Millisecond
}

func DecorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q; %v"
	if status, ok := status.FromError(err); ok {
		return httpgrpc.Errorf(int(status.Code()), errTmpl, field, status.Message())
	}
	return fmt.Errorf(errTmpl, field, err)
}
