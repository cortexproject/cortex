package distributor

import (
	"fmt"
	"net/http"

	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/weaveworks/cortex/util"
)

// PushHandler is a http.Handler which accepts WriteRequests.
func (d *Distributor) PushHandler(w http.ResponseWriter, r *http.Request) {
	var req remote.WriteRequest
	ctx, abort := util.ParseProtoRequest(w, r, &req, true)
	if abort {
		return
	}

	_, err := d.Push(ctx, &req)
	if err != nil {
		log.Errorf("append err: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// UserStats models ingestion statistics for one user.
type UserStats struct {
	IngestionRate float64 `json:"ingestionRate"`
	NumSeries     uint64  `json:"numSeries"`
}

// UserStatsHandler handles user stats to the Distributor.
func (d *Distributor) UserStatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, abort := util.ParseProtoRequest(w, r, nil, false)
	if abort {
		return
	}

	stats, err := d.UserStats(ctx)
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
