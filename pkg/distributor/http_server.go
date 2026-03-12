package distributor

import (
	"net/http"
	"strconv"

	"github.com/cortexproject/cortex/pkg/util"
)

// UserStatsHandler handles user stats to the Distributor.
func (d *Distributor) UserStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := d.UserStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteJSONResponse(w, stats)
}

// TSDBStatusHandler handles TSDB cardinality status requests.
func (d *Distributor) TSDBStatusHandler(w http.ResponseWriter, r *http.Request) {
	limit := int32(10)
	if v := r.FormValue("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = int32(n)
		}
	}

	status, err := d.TSDBStatus(r.Context(), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteJSONResponse(w, status)
}
