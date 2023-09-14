package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/pool"

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

func (r *Retry) Do(ctx context.Context, f func() (*http.Response, error)) (*http.Response, error) {
	if r.maxRetries == 0 {
		// Retries are disabled. Try only once.
		return f()
	}

	tries := 0
	defer func() { r.retriesCount.Observe(float64(tries)) }()

	var (
		resp *http.Response
		err  error
	)
	for ; tries < r.maxRetries; tries++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp, err = f()
		if err != nil && !errors.Is(err, context.Canceled) {
			continue // Retryable
		} else if resp != nil && resp.StatusCode/100 == 5 {
			body, err := tripperware.BodyBuffer(resp, nil)
			if err != nil {
				return nil, err
			}

			if tries < r.maxRetries-1 && isBodyRetryable(yoloString(body)) {
				continue
			}

			resp.Body = &buffer{buff: body, ReadCloser: io.NopCloser(bytes.NewReader(body))}
			resp.ContentLength = int64(len(body))
			return resp, nil
		}
		break
	}
	if err != nil {
		return nil, err
	}
	// We always want to return decoded response body if possible.
	body, err := tripperware.BodyBuffer(resp, nil)
	if err != nil {
		return nil, err
	}
	resp.Body = &buffer{buff: body, ReadCloser: io.NopCloser(bytes.NewReader(body))}
	resp.ContentLength = int64(len(body))
	return resp, err
}

func isBodyRetryable(body string) bool {
	// If pool exhausted, retry at query frontend might make things worse.
	// Rely on retries at querier level only.
	if strings.Contains(body, pool.ErrPoolExhausted.Error()) {
		return false
	}

	return true
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
