package request_tracker

import (
	"net/http"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
	"github.com/google/uuid"
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
	ctx := r.Context()
	if requestmeta.RequestIdFromContext(ctx) == "" {
		reqId := r.Header.Get("X-Request-ID")
		if reqId == "" {
			reqId = uuid.NewString()
		}
		ctx = requestmeta.ContextWithRequestId(ctx, reqId)
		r = r.WithContext(ctx)
	}

	if w.requestTracker != nil {
		insertIndex, err := w.requestTracker.Insert(r.Context(), w.extractor.Extract(r))
		if err == nil {
			defer w.requestTracker.Delete(insertIndex)
		}
	}

	w.handler.ServeHTTP(rw, r)
}
