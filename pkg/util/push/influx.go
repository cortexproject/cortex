package push

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"time"

	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	io2 "github.com/influxdata/influxdb/v2/kit/io"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/middleware"
)

// HandlerForInfluxLine is a http.Handler which accepts Influx Line protocol and converts it to WriteRequests.
func HandlerForInfluxLine(cfg distributor.Config, sourceIPs *middleware.SourceIPExtractor, push func(context.Context, *client.WriteRequest) (*client.WriteResponse, error)) http.Handler {
	return handler(cfg, sourceIPs, push, parseInfluxLineReader)
}

// parseInfluxLineReader parses a Influx Line Protocol request from an io.Reader.
func parseInfluxLineReader(ctx context.Context, r *http.Request, maxSize int, req *client.PreallocWriteRequest) error {
	qp := r.URL.Query()
	precision := qp.Get("precision")
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return fmt.Errorf("precision supplied is not valid: %s", precision)
	}

	encoding := r.Header.Get("Content-Encoding")
	reader, err := batchReadCloser(r.Body, encoding, int64(maxSize))
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err

	}

	points, err := models.ParsePointsWithPrecision(data, time.Now().UTC(), precision)
	if err != nil {
		return err
	}

	return writeRequestFromInfluxPoints(points, req)
}

func writeRequestFromInfluxPoints(points []models.Point, req *client.PreallocWriteRequest) error {
	// Technically the same series should not be repeated. We should put all the samples for
	// a series in single client.Timeseries. Having said that doing it is not very optimal and the
	// occurrence of multiple timestamps for the same series is rare. Only reason I see it happening is
	// for backfilling and this is not the API for that. Keeping that in mind, we are going to create a new
	// client.Timeseries for each sample.

	// Points to Prometheus is heavily inspired from https://github.com/prometheus/influxdb_exporter/blob/a1dc16ad596a990d8854545ea39a57a99a3c7c43/main.go#L148-L211
	for _, pt := range points {
		ts, err := influxPointToTimeseries(pt)
		if err != nil {
			return err
		}
		req.Timeseries = append(req.Timeseries, ts...)
	}

	return nil
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

// batchReadCloser (potentially) wraps an io.ReadCloser in Gzip
// decompression and limits the reading to a specific number of bytes.
func batchReadCloser(rc io.ReadCloser, encoding string, maxBatchSizeBytes int64) (io.ReadCloser, error) {
	switch encoding {
	case "gzip", "x-gzip":
		var err error
		rc, err = gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
	}
	if maxBatchSizeBytes > 0 {
		rc = io2.NewLimitedReadCloser(rc, maxBatchSizeBytes)
	}
	return rc, nil
}
