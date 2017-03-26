package util

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// ParseProtoRequest parses a proto from the body of an HTTP request.
func ParseProtoRequest(ctx context.Context, r *http.Request, req proto.Message, compressed bool) error {
	var reader io.Reader = r.Body
	if compressed {
		reader = snappy.NewReader(r.Body)
	}

	buf := bytes.Buffer{}
	if err := instrument.TimeRequestHistogram(ctx, "util.ParseProtoRequest[decompress]", nil, func(_ context.Context) error {
		_, err := buf.ReadFrom(reader)
		return err
	}); err != nil {
		return err
	}

	if err := instrument.TimeRequestHistogram(ctx, "util.ParseProtoRequest[unmarshal]", nil, func(_ context.Context) error {
		return proto.Unmarshal(buf.Bytes(), req)
	}); err != nil {
		return err
	}

	return nil
}

// SerializeProtoResponse serializes a protobuf response into an HTTP response.
func SerializeProtoResponse(w http.ResponseWriter, resp proto.Message) error {
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error marshaling proto response: %v", err)
	}

	buf := bytes.Buffer{}
	if _, err := snappy.NewWriter(&buf).Write(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error compressing proto response: %v", err)
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error sending proto response: %v", err)
	}
	return nil
}
