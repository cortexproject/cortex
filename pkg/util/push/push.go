package push

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"

	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/influxdata/influxdb/models"
	influx_points "github.com/influxdata/influxdb/v2/http/points"
	v2_models "github.com/influxdata/influxdb/v2/models"
)

// Handler is a http.Handler which accepts WriteRequests.
func Handler(cfg distributor.Config, sourceIPs *middleware.SourceIPExtractor, push func(context.Context, *client.WriteRequest) (*client.WriteResponse, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := util.WithContext(ctx, util.Logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
				logger = util.WithSourceIPs(source, logger)
			}
		}
		compressionType := util.CompressionTypeFor(r.Header.Get("X-Prometheus-Remote-Write-Version"))
		var req client.PreallocWriteRequest
		err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), cfg.MaxRecvMsgSize, &req, compressionType)
		if err != nil {
			level.Error(logger).Log("err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Source == 0 {
			req.Source = client.API
		}

		if _, err := push(ctx, &req.WriteRequest); err != nil {
			resp, ok := httpgrpc.HTTPResponseFromError(err)
			if !ok {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if resp.GetCode() != 202 {
				level.Error(logger).Log("msg", "push error", "err", err)
			}
			http.Error(w, string(resp.Body), int(resp.Code))
		}
	})
}

// HandlerForInfluxLine is a http.Handler which accepts Influx Line protocol and converts it to WriteRequests.
func HandlerForInfluxLine(cfg distributor.Config, sourceIPs *middleware.SourceIPExtractor, push func(context.Context, *client.WriteRequest) (*client.WriteResponse, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := util.WithContext(ctx, util.Logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
				logger = util.WithSourceIPs(source, logger)
			}
		}

		writeReq, err := ParseInfluxLineReader(ctx, r, cfg.MaxRecvMsgSize)
		if err != nil {
			level.Error(logger).Log("err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if writeReq.Source == 0 {
			writeReq.Source = client.API
		}

		if _, err := push(ctx, &writeReq); err != nil {
			resp, ok := httpgrpc.HTTPResponseFromError(err)
			if !ok {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if resp.GetCode() != 202 {
				level.Error(logger).Log("msg", "push error", "err", err)
			}
			http.Error(w, string(resp.Body), int(resp.Code))

			return
		}

		// This is needed else Telegraf will consider a failed write.
		w.WriteHeader(http.StatusNoContent)
	})
}

// ParseInfluxLineReader parses a Influx Line Protocol request from an io.Reader.
func ParseInfluxLineReader(ctx context.Context, r *http.Request, maxSize int) (client.WriteRequest, error) {
	qp := r.URL.Query()
	precision := qp.Get("precision")
	if precision == "" {
		precision = "ns"
	}

	if !v2_models.ValidPrecision(precision) {
		return client.WriteRequest{}, fmt.Errorf("precision supplied is not valid: %s", precision)
	}

	encoding := r.Header.Get("Content-Encoding")
	reader, err := influx_points.BatchReadCloser(r.Body, encoding, int64(maxSize))
	if err != nil {
		return client.WriteRequest{}, err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return client.WriteRequest{}, err

	}

	points, err := models.ParsePointsWithPrecision(data, time.Now().UTC(), precision)
	if err != nil {
		return client.WriteRequest{}, err
	}

	return writeRequestFromInfluxPoints(points)
}

func writeRequestFromInfluxPoints(points []models.Point) (client.WriteRequest, error) {
	// Technically the same series should not be repeated. We should put all the samples for
	// a series in single client.Timeseries. Having said that doing it is not very optimal and the
	// occurence of multiple timestamps for the same series is rare. Only reason I see it happening is
	// for backfilling and this is not the API for that. Keeping that in mind, we are going to create a new
	// client.Timeseries for each sample.

	// Points to Prometheus is heavily inspired from https://github.com/prometheus/influxdb_exporter/blob/a1dc16ad596a990d8854545ea39a57a99a3c7c43/main.go#L148-L211
	req := client.WriteRequest{}
	for _, pt := range points {
		ts, err := influxPointToTimeseries(pt)
		if err != nil {
			return client.WriteRequest{}, err
		}
		req.Timeseries = append(req.Timeseries, ts...)
	}

	return req, nil
}

func influxPointToTimeseries(pt models.Point) ([]client.PreallocTimeseries, error) {
	returnTs := []client.PreallocTimeseries{}

	fields, err := pt.Fields()
	if err != nil {
		return nil, fmt.Errorf("error getting fields from point: %w", err)
	}
	for field, v := range fields {
		var value float64
		switch v := v.(type) {
		case float64:
			value = v
		case int64:
			value = float64(v)
		case bool:
			if v {
				value = 1
			} else {
				value = 0
			}
		default:
			continue
		}

		name := string(pt.Name()) + "_" + field
		if field == "value" {
			name = string(pt.Name())
		}
		replaceInvalidChars(&name)

		tags := pt.Tags()
		lbls := make([]client.LabelAdapter, 0, len(tags)+1) // The additional 1 for __name__.
		lbls = append(lbls, client.LabelAdapter{
			Name:  labels.MetricName,
			Value: name,
		})
		for _, tag := range tags {
			key := string(tag.Key)
			if key == "__name__" {
				continue
			}
			replaceInvalidChars(&key)
			lbls = append(lbls, client.LabelAdapter{
				Name:  key,
				Value: string(tag.Value),
			})
		}
		sort.Slice(lbls, func(i, j int) bool {
			return lbls[i].Name < lbls[j].Name
		})

		returnTs = append(returnTs, client.PreallocTimeseries{
			TimeSeries: &client.TimeSeries{
				Labels: lbls,
				Samples: []client.Sample{{
					TimestampMs: util.TimeToMillis(pt.Time()),
					Value:       value,
				}},
			},
		})
	}

	return returnTs, nil
}

// analog of invalidChars = regexp.MustCompile("[^a-zA-Z0-9_]")
func replaceInvalidChars(in *string) {
	for charIndex, char := range *in {
		charInt := int(char)
		if !((charInt >= 97 && charInt <= 122) || // a-z
			(charInt >= 65 && charInt <= 90) || // A-Z
			(charInt >= 48 && charInt <= 57) || // 0-9
			charInt == 95) { // _

			*in = (*in)[:charIndex] + "_" + (*in)[charIndex+1:]
		}
	}
	// prefix with _ if first char is 0-9
	if int((*in)[0]) >= 48 && int((*in)[0]) <= 57 {
		*in = "_" + *in
	}
}
