package frontend

import (
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
)

var retries = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "query_frontend_retries",
	Help:      "Number of times a request is retried.",
	Buckets:   []float64{0, 1, 2, 3, 4, 5},
})

type retry struct {
	log        log.Logger
	next       http.RoundTripper
	maxRetries int
}

// NewRetryTripperware returns a middleware that retries requests if they
// fail with 500 or a non-HTTP error.
func NewRetryTripperware(log log.Logger, maxRetries int) Tripperware {
	return Tripperware(func(next http.RoundTripper) http.RoundTripper {
		return retry{
			log:        log,
			next:       next,
			maxRetries: maxRetries,
		}
	})
}

func (r retry) RoundTrip(req *http.Request) (*http.Response, error) {
	tries := 0
	defer func() { retries.Observe(float64(tries)) }()

	var lastErr error
	for ; tries < r.maxRetries; tries++ {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		resp, err := r.next.RoundTrip(req)
		if err == nil {
			return resp, nil
		}

		// Retry if we get a HTTP 500 or a non-HTTP error.
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		if !ok || httpResp.Code/100 == 5 {
			lastErr = err
			level.Error(r.log).Log("msg", "error processing request", "try", tries, "err", err)
			continue
		}

		return nil, err
	}
	return nil, lastErr
}
