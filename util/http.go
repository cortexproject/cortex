package util

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/net/context"
)

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

// ParseProtoRequest parses a proto from the body of a http request.
func ParseProtoRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, req proto.Message, compressed bool) error {
	var reader io.Reader = r.Body
	if compressed {
		reader = snappy.NewReader(r.Body)
	}

	buf := bytes.Buffer{}
	if err := instrument.TimeRequestHistogram(ctx, "Distributor.PushHandler[decompress]", nil, func(_ context.Context) error {
		_, err := buf.ReadFrom(reader)
		return err
	}); err != nil {
		return err
	}

	if err := instrument.TimeRequestHistogram(ctx, "Distributor.PushHandler[unmarshall]", nil, func(_ context.Context) error {
		return proto.Unmarshal(buf.Bytes(), req)
	}); err != nil {
		return err
	}

	return nil
}
