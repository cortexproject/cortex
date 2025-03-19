package transport

import (
	"context"
	"errors"
	"strings"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
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
		if err != nil && !errors.Is(err, context.Canceled) {
			continue // Retryable
		} else if resp != nil && resp.Code/100 == 5 {
			// This is not that efficient as we might decode the body multiple
			// times. But error response should be too large so we should be fine.
			// TODO: investigate ways to decode only once.
			body, err := tripperware.BodyBytesFromHTTPGRPCResponse(resp, nil)
			if err != nil {
				return nil, err
			}

			if tries < r.maxRetries-1 && isBodyRetryable(yoloString(body)) {
				continue
			}

			return resp, nil
		}
		break
	}
	if err != nil {
		return nil, err
	}

	return resp, err
}

func isBodyRetryable(body string) bool {
	// If pool exhausted, retry at query frontend might make things worse.
	// Rely on retries at querier level only.
	return !strings.Contains(body, pool.ErrPoolExhausted.Error())
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
