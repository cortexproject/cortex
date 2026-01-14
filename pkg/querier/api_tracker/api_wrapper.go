package api_tracker

import (
	"net/http"
	"strings"
	"time"
)

type APIWrapper struct {
	handler    http.Handler
	apiTracker *APITracker
}

const (
	matchesMaxSize int = 100
)

func NewAPIWrapper(handler http.Handler, apiTracker *APITracker) *APIWrapper {
	return &APIWrapper{
		handler:    handler,
		apiTracker: apiTracker,
	}
}

func (w *APIWrapper) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if w.apiTracker != nil {
		insertIndex, err := w.apiTracker.Insert(r.Context(), makeMapFromRequest(r))
		if err == nil {
			defer w.apiTracker.Delete(insertIndex)
		}
	}

	w.handler.ServeHTTP(rw, r)
}

func makeMapFromRequest(r *http.Request) map[string]interface{} {
	entryMap := make(map[string]interface{})
	entryMap["timestamp_sec"] = time.Now().Unix()
	entryMap["Path"] = r.URL.Path
	entryMap["Method"] = r.Method
	entryMap["User-Agent"] = r.Header.Get("User-Agent")
	entryMap["X-Scope-OrgID"] = r.Header.Get("X-Scope-OrgID")
	entryMap["X-Request-ID"] = r.Header.Get("X-Request-ID")
	entryMap["limit"] = r.URL.Query().Get("limit")
	entryMap["start"] = r.URL.Query().Get("start")
	entryMap["end"] = r.URL.Query().Get("end")

	matches := r.URL.Query()["match[]"]
	matchesStr := strings.Join(matches, ",")
	if len(matchesStr) > matchesMaxSize {
		matchesStr = matchesStr[:matchesMaxSize]
	}
	entryMap["matches"] = matchesStr
	entryMap["number-of-matches"] = len(matches)

	return entryMap
}
