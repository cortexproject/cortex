package push

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
	"github.com/cortexproject/cortex/pkg/util/log"
)

const (
	remoteWriteVersionHeader        = "X-Prometheus-Remote-Write-Version"
	remoteWriteVersion1HeaderValue  = "0.1.0"
	remoteWriteVersion20HeaderValue = "2.0.0"
	appProtoContentType             = "application/x-protobuf"
	appProtoV1ContentType           = "application/x-protobuf;proto=prometheus.WriteRequest"
	appProtoV2ContentType           = "application/x-protobuf;proto=io.prometheus.write.v2.Request"

	rw20WrittenSamplesHeader    = "X-Prometheus-Remote-Write-Samples-Written"
	rw20WrittenHistogramsHeader = "X-Prometheus-Remote-Write-Histograms-Written"
	rw20WrittenExemplarsHeader  = "X-Prometheus-Remote-Write-Exemplars-Written"
)

// Func defines the type of the push. It is similar to http.HandlerFunc.
type Func func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error)

// Handler is a http.Handler which accepts WriteRequests.
func Handler(remoteWrite2Enabled bool, maxRecvMsgSize int, sourceIPs *middleware.SourceIPExtractor, push Func) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := log.WithContext(ctx, log.Logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
				logger = log.WithSourceIPs(source, logger)
			}
		}

		handlePRW1 := func() {
			var req cortexpb.PreallocWriteRequest
			err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, &req, util.RawSnappy)
			if err != nil {
				level.Error(logger).Log("err", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			req.SkipLabelNameValidation = false
			if req.Source == 0 {
				req.Source = cortexpb.API
			}

			if _, err := push(ctx, &req.WriteRequest); err != nil {
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				if !ok {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if resp.GetCode()/100 == 5 {
					level.Error(logger).Log("msg", "push error", "err", err)
				} else if resp.GetCode() != http.StatusAccepted && resp.GetCode() != http.StatusTooManyRequests {
					level.Warn(logger).Log("msg", "push refused", "err", err)
				}
				http.Error(w, string(resp.Body), int(resp.Code))
			}
		}

		handlePRW2 := func() {
			var req writev2.Request
			err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, &req, util.RawSnappy)
			if err != nil {
				level.Error(logger).Log("err", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			v1Req, err := convertV2RequestToV1(&req)
			if err != nil {
				level.Error(logger).Log("err", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			v1Req.SkipLabelNameValidation = false
			if v1Req.Source == 0 {
				v1Req.Source = cortexpb.API
			}

			if resp, err := push(ctx, &v1Req.WriteRequest); err != nil {
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				setPRW2RespHeader(w, 0, 0, 0)
				if !ok {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if resp.GetCode()/100 == 5 {
					level.Error(logger).Log("msg", "push error", "err", err)
				} else if resp.GetCode() != http.StatusAccepted && resp.GetCode() != http.StatusTooManyRequests {
					level.Warn(logger).Log("msg", "push refused", "err", err)
				}
				http.Error(w, string(resp.Body), int(resp.Code))
			} else {
				setPRW2RespHeader(w, resp.Samples, resp.Histograms, resp.Exemplars)
			}
		}

		if remoteWrite2Enabled {
			// follow Prometheus https://github.com/prometheus/prometheus/blob/v3.3.1/storage/remote/write_handler.go#L121
			contentType := r.Header.Get("Content-Type")
			if contentType == "" {
				contentType = appProtoContentType
			}

			msgType, err := parseProtoMsg(contentType)
			if err != nil {
				level.Error(logger).Log("Error decoding remote write request", "err", err)
				http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
				return
			}

			if msgType != config.RemoteWriteProtoMsgV1 && msgType != config.RemoteWriteProtoMsgV2 {
				level.Error(logger).Log("Not accepted msg type", "msgType", msgType, "err", err)
				http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
				return
			}

			enc := r.Header.Get("Content-Encoding")
			if enc == "" {
			} else if enc != compression.Snappy {
				err := fmt.Errorf("%v encoding (compression) is not accepted by this server; only %v is acceptable", enc, compression.Snappy)
				level.Error(logger).Log("Error decoding remote write request", "err", err)
				http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
				return
			}

			switch msgType {
			case config.RemoteWriteProtoMsgV1:
				handlePRW1()
			case config.RemoteWriteProtoMsgV2:
				handlePRW2()
			}
		} else {
			handlePRW1()
		}
	})
}

func setPRW2RespHeader(w http.ResponseWriter, samples, histograms, exemplars int64) {
	w.Header().Set(rw20WrittenSamplesHeader, strconv.FormatInt(samples, 10))
	w.Header().Set(rw20WrittenHistogramsHeader, strconv.FormatInt(histograms, 10))
	w.Header().Set(rw20WrittenExemplarsHeader, strconv.FormatInt(exemplars, 10))
}

// Refer to parseProtoMsg in https://github.com/prometheus/prometheus/blob/main/storage/remote/write_handler.go
func parseProtoMsg(contentType string) (config.RemoteWriteProtoMsg, error) {
	contentType = strings.TrimSpace(contentType)

	parts := strings.Split(contentType, ";")
	if parts[0] != appProtoContentType {
		return "", fmt.Errorf("expected %v as the first (media) part, got %v content-type", appProtoContentType, contentType)
	}
	// Parse potential https://www.rfc-editor.org/rfc/rfc9110#parameter
	for _, p := range parts[1:] {
		pair := strings.Split(p, "=")
		if len(pair) != 2 {
			return "", fmt.Errorf("as per https://www.rfc-editor.org/rfc/rfc9110#parameter expected parameters to be key-values, got %v in %v content-type", p, contentType)
		}
		if pair[0] == "proto" {
			ret := config.RemoteWriteProtoMsg(pair[1])
			if err := ret.Validate(); err != nil {
				return "", fmt.Errorf("got %v content type; %w", contentType, err)
			}
			return ret, nil
		}
	}
	// No "proto=" parameter, assuming v1.
	return config.RemoteWriteProtoMsgV1, nil
}

func convertV2RequestToV1(req *writev2.Request) (cortexpb.PreallocWriteRequest, error) {
	var v1Req cortexpb.PreallocWriteRequest
	v1Timeseries := make([]cortexpb.PreallocTimeseries, 0, len(req.Timeseries))
	var v1Metadata []*cortexpb.MetricMetadata

	b := labels.NewScratchBuilder(0)
	symbols := req.Symbols
	for _, v2Ts := range req.Timeseries {
		lbs := v2Ts.ToLabels(&b, symbols)
		v1Timeseries = append(v1Timeseries, cortexpb.PreallocTimeseries{
			TimeSeries: &cortexpb.TimeSeries{
				Labels:     cortexpb.FromLabelsToLabelAdapters(lbs),
				Samples:    convertV2ToV1Samples(v2Ts.Samples),
				Exemplars:  convertV2ToV1Exemplars(b, symbols, v2Ts.Exemplars),
				Histograms: convertV2ToV1Histograms(v2Ts.Histograms),
			},
		})

		if shouldConvertV2Metadata(v2Ts.Metadata) {
			metricName, err := extract.MetricNameFromLabels(lbs)
			if err != nil {
				return v1Req, err
			}
			v1Metadata = append(v1Metadata, convertV2ToV1Metadata(metricName, symbols, v2Ts.Metadata))
		}
	}

	v1Req.Timeseries = v1Timeseries
	v1Req.Metadata = v1Metadata

	return v1Req, nil
}

func shouldConvertV2Metadata(metadata writev2.Metadata) bool {
	return !(metadata.HelpRef == 0 && metadata.UnitRef == 0 && metadata.Type == writev2.Metadata_METRIC_TYPE_UNSPECIFIED) //nolint:staticcheck
}

func convertV2ToV1Histograms(histograms []writev2.Histogram) []cortexpb.Histogram {
	v1Histograms := make([]cortexpb.Histogram, 0, len(histograms))

	for _, h := range histograms {
		v1Histograms = append(v1Histograms, cortexpb.HistogramWriteV2ProtoToHistogramProto(h))
	}

	return v1Histograms
}

func convertV2ToV1Samples(samples []writev2.Sample) []cortexpb.Sample {
	v1Samples := make([]cortexpb.Sample, 0, len(samples))

	for _, s := range samples {
		v1Samples = append(v1Samples, cortexpb.Sample{
			Value:       s.Value,
			TimestampMs: s.Timestamp,
		})
	}

	return v1Samples
}

func convertV2ToV1Metadata(name string, symbols []string, metadata writev2.Metadata) *cortexpb.MetricMetadata {
	t := cortexpb.UNKNOWN

	switch metadata.Type {
	case writev2.Metadata_METRIC_TYPE_COUNTER:
		t = cortexpb.COUNTER
	case writev2.Metadata_METRIC_TYPE_GAUGE:
		t = cortexpb.GAUGE
	case writev2.Metadata_METRIC_TYPE_HISTOGRAM:
		t = cortexpb.HISTOGRAM
	case writev2.Metadata_METRIC_TYPE_GAUGEHISTOGRAM:
		t = cortexpb.GAUGEHISTOGRAM
	case writev2.Metadata_METRIC_TYPE_SUMMARY:
		t = cortexpb.SUMMARY
	case writev2.Metadata_METRIC_TYPE_INFO:
		t = cortexpb.INFO
	case writev2.Metadata_METRIC_TYPE_STATESET:
		t = cortexpb.STATESET
	}

	return &cortexpb.MetricMetadata{
		Type:             t,
		MetricFamilyName: name,
		Unit:             symbols[metadata.UnitRef],
		Help:             symbols[metadata.HelpRef],
	}
}

func convertV2ToV1Exemplars(b labels.ScratchBuilder, symbols []string, v2Exemplars []writev2.Exemplar) []cortexpb.Exemplar {
	v1Exemplars := make([]cortexpb.Exemplar, 0, len(v2Exemplars))
	for _, e := range v2Exemplars {
		promExemplar := e.ToExemplar(&b, symbols)
		v1Exemplars = append(v1Exemplars, cortexpb.Exemplar{
			Labels:      cortexpb.FromLabelsToLabelAdapters(promExemplar.Labels),
			Value:       e.Value,
			TimestampMs: e.Timestamp,
		})
	}
	return v1Exemplars
}
