package tripperware

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/jsonutil"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/runutil"
)

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: false,
	}.Froze()
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
	MergeResponse(context.Context, Request, ...Response) (Response, error)
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

func decodeSampleStream(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	ss := (*SampleStream)(ptr)
	for field := iter.ReadObject(); field != ""; field = iter.ReadObject() {
		switch field {
		case "metric":
			metricString := iter.ReadAny().ToString()
			lbls := labels.Labels{}
			if err := json.UnmarshalFromString(metricString, &lbls); err != nil {
				iter.ReportError("unmarshal SampleStream", err.Error())
				return
			}
			ss.Labels = cortexpb.FromLabelsToLabelAdapters(lbls)
		case "values":
			for iter.ReadArray() {
				s := cortexpb.Sample{}
				cortexpb.SampleJsoniterDecode(unsafe.Pointer(&s), iter)
				ss.Samples = append(ss.Samples, s)
			}
		case "histograms":
			for iter.ReadArray() {
				h := SampleHistogramPair{}
				UnmarshalSampleHistogramPairJSON(unsafe.Pointer(&h), iter)
				ss.Histograms = append(ss.Histograms, h)
			}
		default:
			iter.ReportError("unmarshal SampleStream", fmt.Sprint("unexpected key:", field))
			return
		}
	}
}

func encodeSampleStream(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	ss := (*SampleStream)(ptr)
	stream.WriteObjectStart()

	stream.WriteObjectField(`metric`)
	lbls, err := cortexpb.FromLabelAdaptersToLabels(ss.Labels).MarshalJSON()
	if err != nil {
		stream.Error = err
		return
	}
	stream.SetBuffer(append(stream.Buffer(), lbls...))

	if len(ss.Samples) > 0 {
		stream.WriteMore()
		stream.WriteObjectField(`values`)
		stream.WriteArrayStart()
		for i, sample := range ss.Samples {
			if i != 0 {
				stream.WriteMore()
			}
			cortexpb.SampleJsoniterEncode(unsafe.Pointer(&sample), stream)
		}
		stream.WriteArrayEnd()
	}

	if len(ss.Histograms) > 0 {
		stream.WriteMore()
		stream.WriteObjectField(`histograms`)
		stream.WriteArrayStart()
		for i, h := range ss.Histograms {
			if i > 0 {
				stream.WriteMore()
			}
			MarshalSampleHistogramPairJSON(unsafe.Pointer(&h), stream)
		}
		stream.WriteArrayEnd()
	}

	stream.WriteObjectEnd()
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
	jsoniter.RegisterTypeEncoderFunc("tripperware.SampleStream", encodeSampleStream, marshalJSONIsEmpty)
	jsoniter.RegisterTypeDecoderFunc("tripperware.SampleStream", decodeSampleStream)
	jsoniter.RegisterTypeEncoderFunc("tripperware.SampleHistogramPair", MarshalSampleHistogramPairJSON, marshalJSONIsEmpty)
	jsoniter.RegisterTypeDecoderFunc("tripperware.SampleHistogramPair", UnmarshalSampleHistogramPairJSON)
}

func marshalJSONIsEmpty(ptr unsafe.Pointer) bool {
	return false
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

func BodyBuffer(res *http.Response, logger log.Logger) ([]byte, error) {
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

	// if the response is gzipped, lets unzip it here
	if strings.EqualFold(res.Header.Get("Content-Encoding"), "gzip") {
		gReader, err := gzip.NewReader(buf)
		if err != nil {
			return nil, err
		}
		defer runutil.CloseWithLogOnErr(logger, gReader, "close gzip reader")

		return io.ReadAll(gReader)
	}

	return buf.Bytes(), nil
}

func BodyBufferFromHTTPGRPCResponse(res *httpgrpc.HTTPResponse, logger log.Logger) ([]byte, error) {
	// if the response is gzipped, lets unzip it here
	headers := http.Header{}
	for _, h := range res.Headers {
		headers[h.Key] = h.Values
	}
	if strings.EqualFold(headers.Get("Content-Encoding"), "gzip") {
		gReader, err := gzip.NewReader(bytes.NewBuffer(res.Body))
		if err != nil {
			return nil, err
		}
		defer runutil.CloseWithLogOnErr(logger, gReader, "close gzip reader")

		return io.ReadAll(gReader)
	}

	return res.Body, nil
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

// Adapted from https://github.com/prometheus/client_golang/blob/4b158abea9470f75b6f07460cdc2189b91914562/api/prometheus/v1/api.go#L84.
func UnmarshalSampleHistogramPairJSON(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	p := (*SampleHistogramPair)(ptr)
	if !iter.ReadArray() {
		iter.ReportError("unmarshal SampleHistogramPair", "SampleHistogramPair must be [timestamp, {histogram}]")
		return
	}
	p.TimestampMs = int64(model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond)))

	if !iter.ReadArray() {
		iter.ReportError("unmarshal SampleHistogramPair", "SamplePair missing histogram")
		return
	}
	for key := iter.ReadObject(); key != ""; key = iter.ReadObject() {
		switch key {
		case "count":
			f, err := strconv.ParseFloat(iter.ReadString(), 64)
			if err != nil {
				iter.ReportError("unmarshal SampleHistogramPair", "count of histogram is not a float")
				return
			}
			p.Histogram.Count = f
		case "sum":
			f, err := strconv.ParseFloat(iter.ReadString(), 64)
			if err != nil {
				iter.ReportError("unmarshal SampleHistogramPair", "sum of histogram is not a float")
				return
			}
			p.Histogram.Sum = f
		case "buckets":
			for iter.ReadArray() {
				b, err := unmarshalHistogramBucket(iter)
				if err != nil {
					iter.ReportError("unmarshal HistogramBucket", err.Error())
					return
				}
				p.Histogram.Buckets = append(p.Histogram.Buckets, b)
			}
		default:
			iter.ReportError("unmarshal SampleHistogramPair", fmt.Sprint("unexpected key in histogram:", key))
			return
		}
	}
	if iter.ReadArray() {
		iter.ReportError("unmarshal SampleHistogramPair", "SampleHistogramPair has too many values, must be [timestamp, {histogram}]")
		return
	}
}

// Adapted from https://github.com/prometheus/client_golang/blob/4b158abea9470f75b6f07460cdc2189b91914562/api/prometheus/v1/api.go#L252.
func unmarshalHistogramBucket(iter *jsoniter.Iterator) (*HistogramBucket, error) {
	b := HistogramBucket{}
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	boundaries, err := iter.ReadNumber().Int64()
	if err != nil {
		return nil, err
	}
	b.Boundaries = int32(boundaries)
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	f, err := strconv.ParseFloat(iter.ReadString(), 64)
	if err != nil {
		return nil, err
	}
	b.Lower = f
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	f, err = strconv.ParseFloat(iter.ReadString(), 64)
	if err != nil {
		return nil, err
	}
	b.Upper = f
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	f, err = strconv.ParseFloat(iter.ReadString(), 64)
	if err != nil {
		return nil, err
	}
	b.Count = f
	if iter.ReadArray() {
		return nil, errors.New("HistogramBucket has too many values, must be [boundaries, lower, upper, count]")
	}
	return &b, nil
}

// Adapted from https://github.com/prometheus/client_golang/blob/4b158abea9470f75b6f07460cdc2189b91914562/api/prometheus/v1/api.go#L137.
func MarshalSampleHistogramPairJSON(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	p := *((*SampleHistogramPair)(ptr))
	stream.WriteArrayStart()
	stream.WriteFloat64(float64(p.TimestampMs) / float64(time.Second/time.Millisecond))
	stream.WriteMore()
	marshalHistogram(p.Histogram, stream)
	stream.WriteArrayEnd()
}

// MarshalHistogram marshals a histogram value using the passed jsoniter stream.
// It writes something like:
//
//	{
//	    "count": "42",
//	    "sum": "34593.34",
//	    "buckets": [
//	      [ 3, "-0.25", "0.25", "3"],
//	      [ 0, "0.25", "0.5", "12"],
//	      [ 0, "0.5", "1", "21"],
//	      [ 0, "2", "4", "6"]
//	    ]
//	}
//
// The 1st element in each bucket array determines if the boundaries are
// inclusive (AKA closed) or exclusive (AKA open):
//
//	0: lower exclusive, upper inclusive
//	1: lower inclusive, upper exclusive
//	2: both exclusive
//	3: both inclusive
//
// The 2nd and 3rd elements are the lower and upper boundary. The 4th element is
// the bucket count.
// Adapted from https://github.com/prometheus/client_golang/blob/4b158abea9470f75b6f07460cdc2189b91914562/api/prometheus/v1/api.go#L329
func marshalHistogram(h SampleHistogram, stream *jsoniter.Stream) {
	stream.WriteObjectStart()
	stream.WriteObjectField(`count`)
	jsonutil.MarshalFloat(h.Count, stream)
	stream.WriteMore()
	stream.WriteObjectField(`sum`)
	jsonutil.MarshalFloat(h.Sum, stream)

	bucketFound := false
	for _, bucket := range h.Buckets {
		if bucket.Count == 0 {
			continue // No need to expose empty buckets in JSON.
		}
		stream.WriteMore()
		if !bucketFound {
			stream.WriteObjectField(`buckets`)
			stream.WriteArrayStart()
		}
		bucketFound = true
		marshalHistogramBucket(*bucket, stream)
	}

	if bucketFound {
		stream.WriteArrayEnd()
	}
	stream.WriteObjectEnd()
}

// marshalHistogramBucket writes something like: [ 3, "-0.25", "0.25", "3"]
// See marshalHistogram to understand what the numbers mean.
// Adapted from https://github.com/prometheus/client_golang/blob/4b158abea9470f75b6f07460cdc2189b91914562/api/prometheus/v1/api.go#L294.
func marshalHistogramBucket(b HistogramBucket, stream *jsoniter.Stream) {
	stream.WriteArrayStart()
	stream.WriteInt32(b.Boundaries)
	stream.WriteMore()
	jsonutil.MarshalFloat(b.Lower, stream)
	stream.WriteMore()
	jsonutil.MarshalFloat(b.Upper, stream)
	stream.WriteMore()
	jsonutil.MarshalFloat(b.Count, stream)
	stream.WriteArrayEnd()
}
