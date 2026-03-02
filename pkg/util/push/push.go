package push

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/schema"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
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

	labelValuePRW1    = "prw1"
	labelValuePRW2    = "prw2"
	labelValueOTLP    = "otlp"
	labelValueUnknown = "unknown"
)

// Func defines the type of the push. It is similar to http.HandlerFunc.
type Func func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error)

// Handler is a http.Handler which accepts WriteRequests.
func Handler(remoteWrite2Enabled bool, acceptUnknownRemoteWriteContentType bool, maxRecvMsgSize int, overrides *validation.Overrides, sourceIPs *middleware.SourceIPExtractor, push Func, requestTotal *prometheus.CounterVec) http.Handler {
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
			userID, err := users.TenantID(ctx)
			if err != nil {
				return
			}

			var req cortexpb.PreallocWriteRequestV2
			// v1 request is put back into the pool by the Distributor.
			defer func() {
				cortexpb.ReuseWriteRequestV2(&req)
				req.Free()
			}()

			err = util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, &req, util.RawSnappy)
			if err != nil {
				level.Error(logger).Log("err", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			req.SkipLabelNameValidation = false
			if req.Source == 0 {
				req.Source = cortexpb.API
			}

			v1Req, err := convertV2RequestToV1(&req, overrides.EnableTypeAndUnitLabels(userID))
			if err != nil {
				level.Error(logger).Log("err", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			v1Req.SkipLabelNameValidation = false
			if v1Req.Source == 0 {
				v1Req.Source = cortexpb.API
			}

			if writeResp, err := push(ctx, &v1Req.WriteRequest); err != nil {
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				if !ok {
					setPRW2RespHeader(w, 0, 0, 0)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				// For the case of HA deduplication, add the stats headers from the push response.
				if writeResp != nil {
					setPRW2RespHeader(w, writeResp.Samples, writeResp.Histograms, writeResp.Exemplars)
				} else {
					setPRW2RespHeader(w, 0, 0, 0)
				}
				if resp.GetCode()/100 == 5 {
					level.Error(logger).Log("msg", "push error", "err", err)
				} else if resp.GetCode() != http.StatusAccepted && resp.GetCode() != http.StatusTooManyRequests {
					level.Warn(logger).Log("msg", "push refused", "err", err)
				}
				http.Error(w, string(resp.Body), int(resp.Code))
			} else {
				setPRW2RespHeader(w, writeResp.Samples, writeResp.Histograms, writeResp.Exemplars)
				w.WriteHeader(http.StatusNoContent)
			}
		}

		// follow Prometheus https://github.com/prometheus/prometheus/blob/v3.3.1/storage/remote/write_handler.go#L121
		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = appProtoContentType
		}

		msgType, err := remote.ParseProtoMsg(contentType)
		contentTypeUnknownOrInvalid := false
		if msgType != remote.WriteV1MessageType && msgType != remote.WriteV2MessageType {
			if acceptUnknownRemoteWriteContentType {
				contentTypeUnknownOrInvalid = true
				msgType = remote.WriteV1MessageType
				level.Debug(logger).Log("msg", "Treating unknown or invalid content-type as remote write v1", "content_type", contentType, "msgType", msgType, "err", err)
			} else {
				if err != nil {
					level.Error(logger).Log("msg", "Content-type header invalid or message type not accepted", "content_type", contentType, "err", err)
					http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
				} else {
					errMsg := fmt.Sprintf("%v protobuf message is not accepted by this server; only accepts %v or %v", msgType, remote.WriteV1MessageType, remote.WriteV2MessageType)
					level.Error(logger).Log("Not accepted msg type", "msgType", msgType)
					http.Error(w, errMsg, http.StatusUnsupportedMediaType)
				}
				return
			}
		}

		if requestTotal != nil {
			requestTotal.WithLabelValues(getTypeLabel(msgType, contentTypeUnknownOrInvalid)).Inc()
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
		case remote.WriteV1MessageType:
			handlePRW1()
		case remote.WriteV2MessageType:
			if !remoteWrite2Enabled {
				errMsg := fmt.Sprintf("%v protobuf message is not accepted by this server; only accepts %v", msgType, remote.WriteV1MessageType)
				http.Error(w, errMsg, http.StatusUnsupportedMediaType)
				return
			}
			handlePRW2()
		}
	})
}

func setPRW2RespHeader(w http.ResponseWriter, samples, histograms, exemplars int64) {
	w.Header().Set(rw20WrittenSamplesHeader, strconv.FormatInt(samples, 10))
	w.Header().Set(rw20WrittenHistogramsHeader, strconv.FormatInt(histograms, 10))
	w.Header().Set(rw20WrittenExemplarsHeader, strconv.FormatInt(exemplars, 10))
}

func convertV2RequestToV1(req *cortexpb.PreallocWriteRequestV2, enableTypeAndUnitLabels bool) (cortexpb.PreallocWriteRequest, error) {
	var v1Req cortexpb.PreallocWriteRequest
	v1Timeseries := make([]cortexpb.PreallocTimeseries, 0, len(req.Timeseries))
	var v1Metadata []*cortexpb.MetricMetadata

	b := labels.NewScratchBuilder(0)
	symbols := req.Symbols
	for _, v2Ts := range req.Timeseries {
		lbs, err := v2Ts.ToLabels(&b, symbols)
		if err != nil {
			return v1Req, err
		}

		if len(v2Ts.Samples) == 0 && len(v2Ts.Histograms) == 0 {
			return v1Req, fmt.Errorf("TimeSeries must contain at least one sample or histogram for series %v", lbs.String())
		}

		unit := symbols[v2Ts.Metadata.UnitRef]
		metricType := v2Ts.Metadata.Type
		shouldAttachTypeAndUnitLabels := enableTypeAndUnitLabels && (metricType != cortexpb.METRIC_TYPE_UNSPECIFIED || unit != "")
		if shouldAttachTypeAndUnitLabels {
			slb := labels.NewScratchBuilder(lbs.Len() + 2) // for __type__ and __unit__
			lbs.Range(func(l labels.Label) {
				// Skip __type__ and __unit__ labels to prevent duplication,
				// We append these labels from metadata.
				if l.Name != model.MetricTypeLabel && l.Name != model.MetricUnitLabel {
					slb.Add(l.Name, l.Value)
				}
			})
			schema.Metadata{Type: cortexpb.MetadataV2MetricTypeToMetricType(metricType), Unit: unit}.AddToLabels(&slb)
			slb.Sort()
			lbs = slb.Labels()
		}

		exemplars, err := convertV2ToV1Exemplars(&b, symbols, v2Ts.Exemplars)
		if err != nil {
			return v1Req, err
		}

		v1Timeseries = append(v1Timeseries, cortexpb.PreallocTimeseries{
			TimeSeries: &cortexpb.TimeSeries{
				Labels:     cortexpb.FromLabelsToLabelAdapters(lbs),
				Samples:    v2Ts.Samples,
				Exemplars:  exemplars,
				Histograms: v2Ts.Histograms,
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

func shouldConvertV2Metadata(metadata cortexpb.MetadataV2) bool {
	return !(metadata.HelpRef == 0 && metadata.UnitRef == 0 && metadata.Type == cortexpb.METRIC_TYPE_UNSPECIFIED) //nolint:staticcheck
}

func convertV2ToV1Metadata(name string, symbols []string, metadata cortexpb.MetadataV2) *cortexpb.MetricMetadata {
	t := cortexpb.UNKNOWN

	switch metadata.Type {
	case cortexpb.METRIC_TYPE_COUNTER:
		t = cortexpb.COUNTER
	case cortexpb.METRIC_TYPE_GAUGE:
		t = cortexpb.GAUGE
	case cortexpb.METRIC_TYPE_HISTOGRAM:
		t = cortexpb.HISTOGRAM
	case cortexpb.METRIC_TYPE_GAUGEHISTOGRAM:
		t = cortexpb.GAUGEHISTOGRAM
	case cortexpb.METRIC_TYPE_SUMMARY:
		t = cortexpb.SUMMARY
	case cortexpb.METRIC_TYPE_INFO:
		t = cortexpb.INFO
	case cortexpb.METRIC_TYPE_STATESET:
		t = cortexpb.STATESET
	}

	return &cortexpb.MetricMetadata{
		Type:             t,
		MetricFamilyName: name,
		Unit:             symbols[metadata.UnitRef],
		Help:             symbols[metadata.HelpRef],
	}
}

func convertV2ToV1Exemplars(b *labels.ScratchBuilder, symbols []string, v2Exemplars []cortexpb.ExemplarV2) ([]cortexpb.Exemplar, error) {
	v1Exemplars := make([]cortexpb.Exemplar, 0, len(v2Exemplars))
	for _, e := range v2Exemplars {
		lbs, err := e.ToLabels(b, symbols)
		if err != nil {
			return nil, err
		}
		v1Exemplars = append(v1Exemplars, cortexpb.Exemplar{
			Labels:      cortexpb.FromLabelsToLabelAdapters(lbs),
			Value:       e.Value,
			TimestampMs: e.Timestamp,
		})
	}
	return v1Exemplars, nil
}

func getTypeLabel(msgType remote.WriteMessageType, unknownOrInvalidContentType bool) string {
	if unknownOrInvalidContentType {
		return labelValueUnknown
	}
	if msgType == remote.WriteV1MessageType {
		return labelValuePRW1
	}
	return labelValuePRW2
}
