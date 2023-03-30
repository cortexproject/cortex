package push

import (
	"io"
	"net/http"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

// OTLPHandler is a http.Handler which accepts OTLP metrics.
func OTLPHandler(sourceIPs *middleware.SourceIPExtractor, push Func) http.Handler {
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

		buf, err := io.ReadAll(r.Body)
		if err != nil {
			level.Error(logger).Log("err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		req := pmetricotlp.NewExportRequest()
		err = req.UnmarshalProto(buf)
		if err != nil {
			level.Error(logger).Log("err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tsMap, err := prometheusremotewrite.FromMetrics(req.Metrics(), prometheusremotewrite.Settings{DisableTargetInfo: true})
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

		tsList := []cortexpb.PreallocTimeseries(nil)
		for _, v := range tsMap {
			tsList = append(tsList, cortexpb.PreallocTimeseries{TimeSeries: &cortexpb.TimeSeries{
				Labels:    makeLabels(v.Labels),
				Samples:   makeSamples(v.Samples),
				Exemplars: makeExemplars(v.Exemplars),
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
