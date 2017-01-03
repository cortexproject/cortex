package ingester

import (
	"net/http"

	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/util"
)

// PushHandler is a http.Handler that accepts proto encoded samples.
func (i *Ingester) PushHandler(w http.ResponseWriter, r *http.Request) {
	var req remote.WriteRequest
	ctx, abort := util.ParseProtoRequest(w, r, &req, true)
	if abort {
		return
	}

	_, err := i.Push(ctx, &req)
	if err != nil {
		switch err {
		case ErrOutOfOrderSample, ErrDuplicateSampleForTimestamp:
			log.Warnf("append err: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			log.Errorf("append err: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// QueryHandler is a http.Handler that accepts protobuf formatted
// query requests and serves them.
func (i *Ingester) QueryHandler(w http.ResponseWriter, r *http.Request) {
	var req cortex.QueryRequest
	ctx, abort := util.ParseProtoRequest(w, r, &req, false)
	if abort {
		return
	}

	resp, err := i.Query(ctx, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteProtoResponse(w, resp)
}

// LabelValuesHandler handles label values
func (i *Ingester) LabelValuesHandler(w http.ResponseWriter, r *http.Request) {
	var req cortex.LabelValuesRequest
	ctx, abort := util.ParseProtoRequest(w, r, &req, false)
	if abort {
		return
	}

	resp, err := i.LabelValues(ctx, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteProtoResponse(w, resp)
}

// UserStatsHandler handles user stats requests to the Ingester.
func (i *Ingester) UserStatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, abort := util.ParseProtoRequest(w, r, nil, false)
	if abort {
		return
	}

	resp, err := i.UserStats(ctx, &cortex.UserStatsRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteProtoResponse(w, resp)
}

// ReadinessHandler returns 204 when the ingester is ready,
// 500 otherwise.  It's used by kubernetes to indicate if the ingester
// pool is ready to have ingesters added / removed.
func (i *Ingester) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	if i.Ready() {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
