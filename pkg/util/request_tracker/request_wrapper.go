package request_tracker

import (
	"net/http"
)

type RequestWrapper struct {
	handler        http.Handler
	requestTracker *RequestTracker
	extractor      Extractor
}

func NewRequestWrapper(handler http.Handler, requestTracker *RequestTracker, extractor Extractor) *RequestWrapper {
	return &RequestWrapper{
		handler:        handler,
		requestTracker: requestTracker,
		extractor:      extractor,
	}
}

func (w *RequestWrapper) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if w.requestTracker != nil {
		insertIndex, err := w.requestTracker.Insert(r.Context(), w.extractor.Extract(r))
		if err == nil {
			defer w.requestTracker.Delete(insertIndex)
		}
	}

	w.handler.ServeHTTP(rw, r)
}
