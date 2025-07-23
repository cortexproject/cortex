package api

import (
	"net/http"

	"github.com/weaveworks/common/httpgrpc"
)

var (
	ErrEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "%s", "end timestamp must not be before start time")
	ErrNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "%s", "zero or negative query resolution step widths are not accepted. Try a positive integer")
	ErrStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "%s", "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
)
