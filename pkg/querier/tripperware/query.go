package tripperware

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// Codec is used to encode/decode query range requests and responses so they can be passed down to middlewares.
type Codec interface {
	Merger
	// DecodeRequest decodes a Request from an http request.
	DecodeRequest(_ context.Context, request *http.Request, forwardHeaders []string) (Request, error)
	// DecodeResponse decodes a Response from an http response.
	// The original request is also passed as a parameter this is useful for implementation that needs the request
	// to merge result or build the result correctly.
	DecodeResponse(context.Context, *http.Response, Request) (Response, error)
	// EncodeRequest encodes a Request into an http request.
	EncodeRequest(context.Context, Request) (*http.Request, error)
	// EncodeResponse encodes a Response into an http response.
	EncodeResponse(context.Context, Response) (*http.Response, error)
}

// Merger is used by middlewares making multiple requests to merge back all responses into a single one.
type Merger interface {
	// MergeResponse merges responses from multiple requests into a single Response
	MergeResponse(...Response) (Response, error)
}

// Response represents a query range response.
type Response interface {
	proto.Message
	// HTTPHeaders returns the HTTP headers in the response.
	HTTPHeaders() map[string][]string
}

// Request represents a query range request that can be process by middlewares.
type Request interface {
	// GetStart returns the start timestamp of the request in milliseconds.
	GetStart() int64
	// GetEnd returns the end timestamp of the request in milliseconds.
	GetEnd() int64
	// GetStep returns the step of the request in milliseconds.
	GetStep() int64
	// GetQuery returns the query of the request.
	GetQuery() string
	// WithStartEnd clone the current request with different start and end timestamp.
	WithStartEnd(startTime int64, endTime int64) Request
	// WithQuery clone the current request with a different query.
	WithQuery(string) Request
	proto.Message
	// LogToSpan writes information about this request to an OpenTracing span
	LogToSpan(opentracing.Span)
	// GetStats returns the stats of the request.
	GetStats() string
	// WithStats clones the current `PrometheusRequest` with a new stats.
	WithStats(stats string) Request
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *SampleStream) UnmarshalJSON(data []byte) error {
	var stream struct {
		Metric model.Metric      `json:"metric"`
		Values []cortexpb.Sample `json:"values"`
	}
	if err := json.Unmarshal(data, &stream); err != nil {
		return err
	}
	s.Labels = cortexpb.FromMetricsToLabelAdapters(stream.Metric)
	s.Samples = stream.Values
	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *SampleStream) MarshalJSON() ([]byte, error) {
	stream := struct {
		Metric model.Metric      `json:"metric"`
		Values []cortexpb.Sample `json:"values"`
	}{
		Metric: cortexpb.FromLabelAdaptersToMetric(s.Labels),
		Values: s.Samples,
	}
	return json.Marshal(stream)
}

func PrometheusResponseQueryableSamplesStatsPerStepJsoniterDecode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	if !iter.ReadArray() {
		iter.ReportError("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", "expected [")
		return
	}

	t := model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond))

	if !iter.ReadArray() {
		iter.ReportError("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", "expected ,")
		return
	}
	v := iter.ReadInt64()

	if iter.ReadArray() {
		iter.ReportError("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", "expected ]")
	}

	*(*PrometheusResponseQueryableSamplesStatsPerStep)(ptr) = PrometheusResponseQueryableSamplesStatsPerStep{
		TimestampMs: int64(t),
		Value:       v,
	}
}

func PrometheusResponseQueryableSamplesStatsPerStepJsoniterEncode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	stats := (*PrometheusResponseQueryableSamplesStatsPerStep)(ptr)
	stream.WriteArrayStart()
	stream.WriteFloat64(float64(stats.TimestampMs) / float64(time.Second/time.Millisecond))
	stream.WriteMore()
	stream.WriteInt64(stats.Value)
	stream.WriteArrayEnd()
}

func init() {
	jsoniter.RegisterTypeEncoderFunc("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", PrometheusResponseQueryableSamplesStatsPerStepJsoniterEncode, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", PrometheusResponseQueryableSamplesStatsPerStepJsoniterDecode)
}

func EncodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// Buffer can be used to read a response body.
// This allows to avoid reading the body multiple times from the `http.Response.Body`.
type Buffer interface {
	Bytes() []byte
}

func BodyBuffer(res *http.Response) ([]byte, error) {
	var buf *bytes.Buffer

	// Attempt to cast the response body to a Buffer and use it if possible.
	// This is because the frontend may have already read the body and buffered it.
	if buffer, ok := res.Body.(Buffer); ok {
		buf = bytes.NewBuffer(buffer.Bytes())
	} else {
		// Preallocate the buffer with the exact size so we don't waste allocations
		// while progressively growing an initial small buffer. The buffer capacity
		// is increased by MinRead to avoid extra allocations due to how ReadFrom()
		// internally works.
		buf = bytes.NewBuffer(make([]byte, 0, res.ContentLength+bytes.MinRead))
		if _, err := buf.ReadFrom(res.Body); err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
		}
	}

	// if the response is gziped, lets unzip it here
	if strings.EqualFold(res.Header.Get("Content-Encoding"), "gzip") {
		gReader, err := gzip.NewReader(buf)

		if err != nil {
			return nil, err
		}

		return io.ReadAll(gReader)
	}

	return buf.Bytes(), nil
}

func StatsMerge(stats map[int64]*PrometheusResponseQueryableSamplesStatsPerStep) *PrometheusResponseStats {
	keys := make([]int64, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	result := &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}}
	for _, key := range keys {
		result.Samples.TotalQueryableSamplesPerStep = append(result.Samples.TotalQueryableSamplesPerStep, stats[key])
		result.Samples.TotalQueryableSamples += stats[key].Value
	}

	return result
}
