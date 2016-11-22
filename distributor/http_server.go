package distributor

import (
	"net/http"

	"github.com/prometheus/common/log"
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
		switch e := err.(type) {
		case IngesterError:
			switch {
			case 400 <= e.StatusCode && e.StatusCode < 500:
				log.Warnf("push err: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
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
