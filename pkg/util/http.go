package util

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"

	"github.com/blang/semver"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"gopkg.in/yaml.v2"
)

const messageSizeLargerErrFmt = "received message larger than max (%d vs %d)"

// WriteJSONResponse writes some JSON as a HTTP response.
func WriteJSONResponse(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")

	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We ignore errors here, because we cannot do anything about them.
	// Write will trigger sending Status code, so we cannot send a different status code afterwards.
	// Also this isn't internal error, but error communicating with client.
	_, _ = w.Write(data)
}

// WriteYAMLResponse writes some YAML as a HTTP response.
func WriteYAMLResponse(w http.ResponseWriter, v interface{}) {
	// There is not standardised content-type for YAML, text/plain ensures the
	// YAML is displayed in the browser instead of offered as a download
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	data, err := yaml.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We ignore errors here, because we cannot do anything about them.
	// Write will trigger sending Status code, so we cannot send a different status code afterwards.
	// Also this isn't internal error, but error communicating with client.
	_, _ = w.Write(data)
}

// Sends message as text/plain response with 200 status code.
func WriteTextResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/plain")

	// Ignore inactionable errors.
	_, _ = w.Write([]byte(message))
}

// RenderHTTPResponse either responds with json or a rendered html page using the passed in template
// by checking the Accepts header
func RenderHTTPResponse(w http.ResponseWriter, v interface{}, t *template.Template, r *http.Request) {
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "application/json") {
		WriteJSONResponse(w, v)
		return
	}

	err := t.Execute(w, v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// CompressionType for encoding and decoding requests and responses.
type CompressionType int

// Values for CompressionType
const (
	NoCompression CompressionType = iota
	FramedSnappy
	RawSnappy
)

var rawSnappyFromVersion = semver.MustParse("0.1.0")

// CompressionTypeFor a given version of the Prometheus remote storage protocol.
// See https://github.com/prometheus/prometheus/issues/2692.
func CompressionTypeFor(version string) CompressionType {
	ver, err := semver.Make(version)
	if err != nil {
		return FramedSnappy
	}

	if ver.GTE(rawSnappyFromVersion) {
		return RawSnappy
	}
	return FramedSnappy
}

// ParseProtoReader parses a compressed proto from an io.Reader.
func ParseProtoReader(ctx context.Context, reader io.Reader, expectedSize, maxSize int, req proto.Message, compression CompressionType) error {
	body, err := decompressRequest(ctx, reader, expectedSize, maxSize, compression)
	if err != nil {
		return err
	}

	// We re-implement proto.Unmarshal here as it calls XXX_Unmarshal first,
	// which we can't override without upsetting golint.
	req.Reset()
	if u, ok := req.(proto.Unmarshaler); ok {
		err = u.Unmarshal(body)
	} else {
		err = proto.NewBuffer(body).Unmarshal(req)
	}
	if err != nil {
		return err
	}

	return nil
}

func decompressRequest(ctx context.Context, reader io.Reader, expectedSize, maxSize int, compression CompressionType) (body []byte, err error) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		sp.LogFields(otlog.String("event", "util.ParseProtoRequest[start reading]"))
		defer func() {
			sp.LogFields(otlog.String("event", "util.ParseProtoRequest[unmarshal]"),
				otlog.Int("size", len(body)))
		}()
	}
	defer func() {
		if len(body) > maxSize {
			err = fmt.Errorf(messageSizeLargerErrFmt, len(body), maxSize)
		}
	}()
	buffer, ok := tryBufferFromReader(reader)
	if ok {
		body, err = decompressFromBuffer(buffer, maxSize, compression, sp)
		return
	}
	body, err = decompressFromReader(reader, expectedSize, maxSize, compression, sp)
	return
}

func decompressFromReader(reader io.Reader, expectedSize, maxSize int, compression CompressionType, sp opentracing.Span) ([]byte, error) {
	var (
		buf  bytes.Buffer
		body []byte
		err  error
	)
	if expectedSize > 0 {
		if expectedSize > maxSize {
			return nil, fmt.Errorf(messageSizeLargerErrFmt, expectedSize, maxSize)
		}
		buf.Grow(expectedSize + bytes.MinRead) // extra space guarantees no reallocation
	}
	switch compression {
	case NoCompression:
		// Read from LimitReader with limit max+1. So if the underlying
		// reader is over limit, the result will be bigger than max.
		_, err = buf.ReadFrom(io.LimitReader(reader, int64(maxSize)+1))
		body = buf.Bytes()
	case FramedSnappy:
		_, err = buf.ReadFrom(io.LimitReader(snappy.NewReader(reader), int64(maxSize)+1))
		body = buf.Bytes()
	case RawSnappy:
		_, err = buf.ReadFrom(reader)
		body = buf.Bytes()
		if sp != nil {
			sp.LogFields(otlog.String("event", "util.ParseProtoRequest[decompress]"),
				otlog.Int("size", len(body)))
		}
		if err != nil {
			return nil, err
		}
		if len(body) > maxSize {
			return nil, fmt.Errorf(messageSizeLargerErrFmt, len(body), maxSize)
		}
		var size int
		size, err = snappy.DecodedLen(body)
		if err != nil {
			return nil, err
		}
		if size > maxSize {
			return nil, fmt.Errorf(messageSizeLargerErrFmt, size, maxSize)
		}
		if err == nil {
			body, err = snappy.Decode(nil, body)
		}
	}
	return body, err
}

func decompressFromBuffer(buffer *bytes.Buffer, maxSize int, compression CompressionType, sp opentracing.Span) ([]byte, error) {
	if len(buffer.Bytes()) > maxSize {
		return nil, fmt.Errorf(messageSizeLargerErrFmt, len(buffer.Bytes()), maxSize)
	}

	switch compression {
	case NoCompression:
		return buffer.Bytes(), nil
	case FramedSnappy:
		decompressed := bytes.NewBuffer(make([]byte, 0, len(buffer.Bytes())))
		_, err := decompressed.ReadFrom(io.LimitReader(snappy.NewReader(buffer), int64(maxSize)+1))
		if err != nil {
			return nil, err
		}
		return decompressed.Bytes(), nil
	case RawSnappy:
		if sp != nil {
			sp.LogFields(otlog.String("event", "util.ParseProtoRequest[decompress]"),
				otlog.Int("size", len(buffer.Bytes())))
		}
		size, err := snappy.DecodedLen(buffer.Bytes())
		if err != nil {
			return nil, err
		}
		if size > maxSize {
			return nil, fmt.Errorf(messageSizeLargerErrFmt, size, maxSize)
		}
		body, err := snappy.Decode(nil, buffer.Bytes())
		if err != nil {
			return nil, err
		}
		return body, nil
	}
	return nil, nil
}

// tryBufferFromReader attempts to cast the reader to a `*bytes.Buffer` this is possible when using httpgrpc.
// If it fails it will return nil and false.
func tryBufferFromReader(reader io.Reader) (*bytes.Buffer, bool) {
	if bufReader, ok := reader.(interface {
		BytesBuffer() *bytes.Buffer
	}); ok {
		return bufReader.BytesBuffer(), true
	}
	return nil, false
}

// SerializeProtoResponse serializes a protobuf response into an HTTP response.
func SerializeProtoResponse(w http.ResponseWriter, resp proto.Message, compression CompressionType) error {
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error marshaling proto response: %v", err)
	}

	switch compression {
	case NoCompression:
	case FramedSnappy:
		buf := bytes.Buffer{}
		writer := snappy.NewBufferedWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return err
		}
		writer.Close()
		data = buf.Bytes()
	case RawSnappy:
		data = snappy.Encode(nil, data)
	}

	if _, err := w.Write(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error sending proto response: %v", err)
	}
	return nil
}
