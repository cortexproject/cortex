package querier

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/prometheus/prometheus/scrape"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	defaultLimit = -1
)

type MetadataQuerier interface {
	MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error)
}

type metricMetadata struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

const (
	statusSuccess = "success"
	statusError   = "error"
)

type metadataSuccessResult struct {
	Status string                      `json:"status"`
	Data   map[string][]metricMetadata `json:"data"`
}

type metadataErrorResult struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

// MetadataHandler returns metric metadata held by Cortex for a given tenant.
// It is kept and returned as a set.
func MetadataHandler(m MetadataQuerier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit, err := validateLimits("limit", w, r)
		if err != nil {
			return
		}

		limitPerMetric, err := validateLimits("limit_per_metric", w, r)
		if err != nil {
			return
		}

		metric := r.FormValue("metric")
		req := &client.MetricsMetadataRequest{
			Limit:          int64(limit),
			LimitPerMetric: int64(limitPerMetric),
			Metric:         metric,
		}

		resp, err := m.MetricsMetadata(r.Context(), req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			util.WriteJSONResponse(w, metadataErrorResult{Status: statusError, Error: err.Error()})
			return
		}

		// Put all the elements of the pseudo-set into a map of slices for marshalling.
		metrics := map[string][]metricMetadata{}
		for _, m := range resp {
			ms, ok := metrics[m.MetricFamily]
			// We have to check limit both ingester and here since the ingester only check
			// for one user, it cannot handle the case when the mergeMetadataQuerier
			// (tenant-federation) is used.
			if limitPerMetric > 0 && len(ms) >= limitPerMetric {
				continue
			}

			if !ok {
				if limit >= 0 && len(metrics) >= limit {
					break
				}
				// Most metrics will only hold 1 copy of the same metadata.
				ms = make([]metricMetadata, 0, 1)
				metrics[m.MetricFamily] = ms
			}
			metrics[m.MetricFamily] = append(ms, metricMetadata{Type: string(m.Type), Help: m.Help, Unit: m.Unit})
		}

		util.WriteJSONResponse(w, metadataSuccessResult{Status: statusSuccess, Data: metrics})
	})
}

func validateLimits(name string, w http.ResponseWriter, r *http.Request) (int, error) {
	v := defaultLimit
	if s := r.FormValue(name); s != "" {
		var err error
		if v, err = strconv.Atoi(s); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			util.WriteJSONResponse(w, metadataErrorResult{Status: statusError, Error: fmt.Sprintf("%s must be a number", name)})
			return 0, err
		}
	}
	return v, nil
}
