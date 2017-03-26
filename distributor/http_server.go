package distributor

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/promql"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/util"
)

// PushHandler is a http.Handler which accepts WriteRequests.
func (d *Distributor) PushHandler(w http.ResponseWriter, r *http.Request) {
	var req cortex.WriteRequest
	if err := util.ParseProtoRequest(r.Context(), w, r, &req, true); err != nil {
		log.Errorf(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, err := d.Push(r.Context(), &req); err != nil {
		if grpc.Code(err) == codes.ResourceExhausted {
			switch grpc.ErrorDesc(err) {
			case util.ErrUserSeriesLimitExceeded.Error():
				err = util.ErrUserSeriesLimitExceeded
			case util.ErrMetricSeriesLimitExceeded.Error():
				err = util.ErrMetricSeriesLimitExceeded
			}
		}

		var code int
		switch err {
		case errIngestionRateLimitExceeded, util.ErrUserSeriesLimitExceeded, util.ErrMetricSeriesLimitExceeded:
			code = http.StatusTooManyRequests
		default:
			code = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), code)
		log.Errorf("append err: %v", err)
	}
}

// UserStats models ingestion statistics for one user.
type UserStats struct {
	IngestionRate float64 `json:"ingestionRate"`
	NumSeries     uint64  `json:"numSeries"`
}

// UserStatsHandler handles user stats to the Distributor.
func (d *Distributor) UserStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := d.UserStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteJSONResponse(w, stats)
}

// ValidateExprHandler validates a PromQL expression.
func (d *Distributor) ValidateExprHandler(w http.ResponseWriter, r *http.Request) {
	_, err := promql.ParseExpr(r.FormValue("expr"))

	// We mimick the response format of Prometheus's official API here for
	// consistency, but unfortunately its private types (string consts etc.)
	// aren't reusable.
	if err == nil {
		util.WriteJSONResponse(w, map[string]string{
			"status": "success",
		})
		return
	}

	parseErr, ok := err.(*promql.ParseErr)
	if !ok {
		// This should always be a promql.ParseErr.
		http.Error(w, fmt.Sprintf("unexpected error returned from PromQL parser: %v", err), http.StatusInternalServerError)
		return
	}

	// If the parsing input was a single line, parseErr.Line is 0
	// and the generated error string omits the line entirely. But we
	// want to report line numbers consistently, no matter how many
	// lines there are (starting at 1).
	if parseErr.Line == 0 {
		parseErr.Line = 1
	}
	w.WriteHeader(http.StatusBadRequest)
	util.WriteJSONResponse(w, map[string]interface{}{
		"status":    "error",
		"errorType": "bad_data",
		"error":     err.Error(),
		"location": map[string]int{
			"line": parseErr.Line,
			"pos":  parseErr.Pos,
		},
	})
}
