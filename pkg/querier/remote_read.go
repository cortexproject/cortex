package querier

import (
	"net/http"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
)

// RemoteReadHandler handles Prometheus remote read requests.
func RemoteReadHandler(q storage.Queryable) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressionType := util.CompressionTypeFor(r.Header.Get("X-Prometheus-Remote-Read-Version"))

		ctx := r.Context()
		var req client.ReadRequest
		logger := util.WithContext(r.Context(), util.Logger)
		if _, err := util.ParseProtoReader(ctx, r.Body, &req, compressionType); err != nil {
			level.Error(logger).Log("err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Fetch samples for all queries in parallel.
		resp := client.ReadResponse{
			Results: make([]*client.QueryResponse, len(req.Queries)),
		}
		errors := make(chan error)
		for i, qr := range req.Queries {
			go func(i int, qr *client.QueryRequest) {
				from, to, matchers, err := client.FromQueryRequest(qr)
				if err != nil {
					errors <- err
					return
				}

				querier, err := q.Querier(ctx, int64(from), int64(to))
				if err != nil {
					errors <- err
					return
				}

				seriesSet, err := querier.Select(nil, matchers...)
				if err != nil {
					errors <- err
					return
				}

				matrix, err := seriesSetToMatrix(seriesSet)
				if err != nil {
					errors <- err
					return
				}

				resp.Results[i] = client.ToQueryResponse(matrix)
				errors <- nil
			}(i, qr)
		}

		var lastErr error
		for range req.Queries {
			err := <-errors
			if err != nil {
				lastErr = err
			}
		}
		if lastErr != nil {
			http.Error(w, lastErr.Error(), http.StatusBadRequest)
			return
		}

		if err := util.SerializeProtoResponse(w, &resp, compressionType); err != nil {
			level.Error(logger).Log("msg", "error sending remote read response", "err", err)
		}
	})
}

func seriesSetToMatrix(s storage.SeriesSet) (model.Matrix, error) {
	result := model.Matrix{}

	for s.Next() {
		series := s.At()
		values := []model.SamplePair{}
		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			values = append(values, model.SamplePair{
				Timestamp: model.Time(t),
				Value:     model.SampleValue(v),
			})
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
		result = append(result, &model.SampleStream{
			Metric: labelsToMetric(series.Labels()),
			Values: values,
		})
	}

	return result, s.Err()
}
