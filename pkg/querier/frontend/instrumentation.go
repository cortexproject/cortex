package frontend

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/instrument"
)

var queryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "frontend_rt_duration_seconds",
	Help:      "Total time spent in seconds roundtripping requests.",
	Buckets:   prometheus.DefBuckets,
}, []string{"name", "status_code"})

// InstrumentRoundTripper can be inserted into the tripperware chain to expose timing information.
func InstrumentRoundTripper(name string, next http.RoundTripper) http.RoundTripper {
	return RoundTripFunc(func(req *http.Request) (*http.Response, error) {
		var resp *http.Response
		err := instrument.TimeRequestHistogram(req.Context(), name, queryDuration, func(context.Context) error {
			var err error
			resp, err = next.RoundTrip(req)
			return err
		})
		return resp, err
	})
}
