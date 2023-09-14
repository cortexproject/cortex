package transport

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
)

type Retry struct {
	maxRetries   int
	retriesCount prometheus.Histogram
}

func NewRetry(maxRetries int, reg prometheus.Registerer) *Retry {
	return &Retry{
		maxRetries: maxRetries,
		retriesCount: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "query_frontend_retries",
			Help:      "Number of times a request is retried.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5},
		}),
	}
}

func (r *Retry) Do(ctx context.Context, f func() (*httpgrpc.HTTPResponse, error)) (*httpgrpc.HTTPResponse, error) {
	if r.maxRetries == 0 {
		// Retries are disabled. Try only once.
		return f()
	}

	tries := 0
	defer func() { r.retriesCount.Observe(float64(tries)) }()

	var (
		resp *httpgrpc.HTTPResponse
		err  error
	)
	for ; tries < r.maxRetries; tries++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp, err = f()
		if err != nil && err != context.Canceled {
			continue // Retryable
		} else if resp != nil && resp.Code/100 == 5 {
			continue // Retryable
		} else {
			break
		}
	}
	return resp, err
}
