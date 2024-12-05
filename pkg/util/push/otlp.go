package push

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"
)

// OTLPHandler is a http.Handler which accepts OTLP metrics.
func OTLPHandler(maxRecvMsgSize int, overrides *validation.Overrides, cfg distributor.OTLPConfig, sourceIPs *middleware.SourceIPExtractor, push Func) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := util_log.WithContext(ctx, util_log.Logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
				logger = util_log.WithSourceIPs(source, logger)
			}
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return
		}

		req, err := decodeOTLPWriteRequest(ctx, r, maxRecvMsgSize)
		if err != nil {
			level.Error(logger).Log("err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		prwReq := cortexpb.WriteRequest{
			Source:                  cortexpb.API,
			Metadata:                nil,
			SkipLabelNameValidation: false,
		}

		// otlp to prompb TimeSeries
		promTsList, err := convertToPromTS(r.Context(), req.Metrics(), cfg, overrides, userID, logger)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// convert prompb to cortexpb TimeSeries
		tsList := []cortexpb.PreallocTimeseries(nil)
		for _, v := range promTsList {
			tsList = append(tsList, cortexpb.PreallocTimeseries{TimeSeries: &cortexpb.TimeSeries{
				Labels:     makeLabels(v.Labels),
				Samples:    makeSamples(v.Samples),
				Exemplars:  makeExemplars(v.Exemplars),
				Histograms: makeHistograms(v.Histograms),
			}})
		}
		prwReq.Timeseries = tsList

		if _, err := push(ctx, &prwReq); err != nil {
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
	})
}

func decodeOTLPWriteRequest(ctx context.Context, r *http.Request, maxSize int) (pmetricotlp.ExportRequest, error) {
	expectedSize := int(r.ContentLength)
	if expectedSize > maxSize {
		return pmetricotlp.NewExportRequest(), fmt.Errorf("received message larger than max (%d vs %d)", expectedSize, maxSize)
	}

	contentType := r.Header.Get("Content-Type")
	contentEncoding := r.Header.Get("Content-Encoding")

	var compressionType util.CompressionType
	switch contentEncoding {
	case "gzip":
		compressionType = util.Gzip
	case "":
		compressionType = util.NoCompression
	default:
		return pmetricotlp.NewExportRequest(), fmt.Errorf("unsupported compression: %s, Supported compression types are \"gzip\" or '' (no compression)", contentEncoding)
	}

	var decoderFunc func(reader io.Reader) (pmetricotlp.ExportRequest, error)
	switch contentType {
	case pbContentType:
		decoderFunc = func(reader io.Reader) (pmetricotlp.ExportRequest, error) {
			req := pmetricotlp.NewExportRequest()
			otlpReqProto := otlpProtoMessage{req: &req}
			return req, util.ParseProtoReader(ctx, reader, expectedSize, maxSize, otlpReqProto, compressionType)
		}
	case jsonContentType:
		decoderFunc = func(reader io.Reader) (pmetricotlp.ExportRequest, error) {
			req := pmetricotlp.NewExportRequest()

			reader = io.LimitReader(reader, int64(maxSize)+1)
			if compressionType == util.Gzip {
				var err error
				reader, err = gzip.NewReader(reader)
				if err != nil {
					return req, err
				}
			}

			var buf bytes.Buffer
			if expectedSize > 0 {
				buf.Grow(expectedSize + bytes.MinRead) // extra space guarantees no reallocation
			}
			_, err := buf.ReadFrom(reader)
			if err != nil {
				return req, err
			}

			return req, req.UnmarshalJSON(buf.Bytes())
		}
	default:
		return pmetricotlp.NewExportRequest(), fmt.Errorf("unsupported content type: %s, supported: [%s, %s]", contentType, jsonContentType, pbContentType)
	}

	return decoderFunc(r.Body)
}

func convertToPromTS(ctx context.Context, pmetrics pmetric.Metrics, cfg distributor.OTLPConfig, overrides *validation.Overrides, userID string, logger log.Logger) ([]prompb.TimeSeries, error) {
	promConverter := prometheusremotewrite.NewPrometheusConverter()
	settings := prometheusremotewrite.Settings{
		AddMetricSuffixes: true,
		DisableTargetInfo: cfg.DisableTargetInfo,
	}

	var annots annotations.Annotations
	var err error

	if cfg.ConvertAllAttributes {
		annots, err = promConverter.FromMetrics(ctx, convertToMetricsAttributes(pmetrics), settings)
	} else {
		settings.PromoteResourceAttributes = overrides.PromoteResourceAttributes(userID)
		annots, err = promConverter.FromMetrics(ctx, pmetrics, settings)
	}

	ws, _ := annots.AsStrings("", 0, 0)
	if len(ws) > 0 {
		level.Warn(logger).Log("msg", "Warnings translating OTLP metrics to Prometheus write request", "warnings", ws)
	}

	if err != nil {
		level.Error(logger).Log("msg", "Error translating OTLP metrics to Prometheus write request", "err", err)
		return nil, err
	}
	return promConverter.TimeSeries(), nil
}

func makeLabels(in []prompb.Label) []cortexpb.LabelAdapter {
	out := make(labels.Labels, 0, len(in))
	for _, l := range in {
		out = append(out, labels.Label{Name: l.Name, Value: l.Value})
	}
	return cortexpb.FromLabelsToLabelAdapters(out)
}

func makeSamples(in []prompb.Sample) []cortexpb.Sample {
	out := make([]cortexpb.Sample, 0, len(in))
	for _, s := range in {
		out = append(out, cortexpb.Sample{
			Value:       s.Value,
			TimestampMs: s.Timestamp,
		})
	}
	return out
}

func makeExemplars(in []prompb.Exemplar) []cortexpb.Exemplar {
	out := make([]cortexpb.Exemplar, 0, len(in))
	for _, e := range in {
		out = append(out, cortexpb.Exemplar{
			Labels:      makeLabels(e.Labels),
			Value:       e.Value,
			TimestampMs: e.Timestamp,
		})
	}
	return out
}

func makeHistograms(in []prompb.Histogram) []cortexpb.Histogram {
	out := make([]cortexpb.Histogram, 0, len(in))
	for _, h := range in {
		out = append(out, cortexpb.HistogramPromProtoToHistogramProto(h))
	}
	return out
}

func convertToMetricsAttributes(md pmetric.Metrics) pmetric.Metrics {
	cloneMd := pmetric.NewMetrics()
	md.CopyTo(cloneMd)
	rms := cloneMd.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		resource := rms.At(i).Resource()

		ilms := rms.At(i).ScopeMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ilm := ilms.At(j)
			metricSlice := ilm.Metrics()
			for k := 0; k < metricSlice.Len(); k++ {
				addAttributesToMetric(metricSlice.At(k), resource.Attributes())
			}
		}
	}
	return cloneMd
}

// addAttributesToMetric adds additional labels to the given metric
func addAttributesToMetric(metric pmetric.Metric, labelMap pcommon.Map) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		addAttributesToNumberDataPoints(metric.Gauge().DataPoints(), labelMap)
	case pmetric.MetricTypeSum:
		addAttributesToNumberDataPoints(metric.Sum().DataPoints(), labelMap)
	case pmetric.MetricTypeHistogram:
		addAttributesToHistogramDataPoints(metric.Histogram().DataPoints(), labelMap)
	case pmetric.MetricTypeSummary:
		addAttributesToSummaryDataPoints(metric.Summary().DataPoints(), labelMap)
	case pmetric.MetricTypeExponentialHistogram:
		addAttributesToExponentialHistogramDataPoints(metric.ExponentialHistogram().DataPoints(), labelMap)
	}
}

func addAttributesToNumberDataPoints(ps pmetric.NumberDataPointSlice, newAttributeMap pcommon.Map) {
	for i := 0; i < ps.Len(); i++ {
		joinAttributeMaps(newAttributeMap, ps.At(i).Attributes())
	}
}

func addAttributesToHistogramDataPoints(ps pmetric.HistogramDataPointSlice, newAttributeMap pcommon.Map) {
	for i := 0; i < ps.Len(); i++ {
		joinAttributeMaps(newAttributeMap, ps.At(i).Attributes())
	}
}

func addAttributesToSummaryDataPoints(ps pmetric.SummaryDataPointSlice, newAttributeMap pcommon.Map) {
	for i := 0; i < ps.Len(); i++ {
		joinAttributeMaps(newAttributeMap, ps.At(i).Attributes())
	}
}

func addAttributesToExponentialHistogramDataPoints(ps pmetric.ExponentialHistogramDataPointSlice, newAttributeMap pcommon.Map) {
	for i := 0; i < ps.Len(); i++ {
		joinAttributeMaps(newAttributeMap, ps.At(i).Attributes())
	}
}

func joinAttributeMaps(from, to pcommon.Map) {
	from.Range(func(k string, v pcommon.Value) bool {
		v.CopyTo(to.PutEmpty(k))
		return true
	})
}

// otlpProtoMessage Implements proto.Meesage, proto.Unmarshaler
type otlpProtoMessage struct {
	req *pmetricotlp.ExportRequest
}

func (otlpProtoMessage) ProtoMessage() {}

func (otlpProtoMessage) Reset() {}

func (otlpProtoMessage) String() string { return "" }

func (o otlpProtoMessage) Unmarshal(data []byte) error { return o.req.UnmarshalProto(data) }
