package util

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/log"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/user"
)

// ParseProtoRequest parses a proto from the body of a http request.
func ParseProtoRequest(w http.ResponseWriter, r *http.Request, req proto.Message, compressed bool) (ctx context.Context, abort bool) {
	userID := r.Header.Get(user.OrgIDHeaderName)
	if userID == "" {
		http.Error(w, "", http.StatusUnauthorized)
		return nil, true
	}

	ctx = user.WithID(r.Context(), userID)
	if req == nil {
		return ctx, false
	}

	var reader io.Reader = r.Body
	if compressed {
		reader = snappy.NewReader(r.Body)
	}

	buf := bytes.Buffer{}
	if _, err := buf.ReadFrom(reader); err != nil {
		log.Errorf(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, true
	}

	if err := proto.Unmarshal(buf.Bytes(), req); err != nil {
		log.Errorf(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, true
	}

	return ctx, false
}

// WriteJSONResponse writes some JSON as a HTTP response.
func WriteJSONResponse(w http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err = w.Write(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
}

// WriteProtoResponse writes a proto as a HTTP response.
func WriteProtoResponse(w http.ResponseWriter, resp proto.Message) {
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err = w.Write(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// TODO: set Content-type.
}
