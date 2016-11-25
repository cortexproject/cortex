package ingester

import (
	"net/http"
)

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
