package limiter

import (
	"context"
	"fmt"

	"go.uber.org/atomic"
)

type responseSizeLimiterCtxKey struct{}

var (
	responseLimiterCtxKey = &responseSizeLimiterCtxKey{}
	ErrMaxResponseSizeHit = "the query response size exceeds limit (limit: %d bytes)"
)

type ResponseSizeLimiter struct {
	responseSizeCount atomic.Int64
	maxResponseSize   int64
}

// NewResponseSizeLimiter creates a new limiter to track total response size.
func NewResponseSizeLimiter(maxResponseSize int64) *ResponseSizeLimiter {
	return &ResponseSizeLimiter{
		maxResponseSize: maxResponseSize,
	}
}

func AddResponseSizeLimiterToContext(ctx context.Context, responseSizeLimiter *ResponseSizeLimiter) context.Context {
	return context.WithValue(ctx, responseLimiterCtxKey, responseSizeLimiter)
}

// ResponseSizeLimiterFromContextWithFallback returns a ResponseSizeLimiter from the current context.
// If there is not a ResponseSizeLimiter on the context it will return a new no-op limiter.
func ResponseSizeLimiterFromContextWithFallback(ctx context.Context) *ResponseSizeLimiter {
	rl, ok := ctx.Value(responseLimiterCtxKey).(*ResponseSizeLimiter)
	if !ok {
		// If there's no limiter return a new unlimited limiter as a fallback
		rl = NewResponseSizeLimiter(0)
	}
	return rl
}

// AddResponseBytes adds response bytes received at query-frontend to the total query response size
// and returns an error if the limit is reached.
func (rl *ResponseSizeLimiter) AddResponseBytes(responseSizeInBytes int) error {
	if rl.maxResponseSize == 0 {
		return nil
	}
	if rl.responseSizeCount.Add(int64(responseSizeInBytes)) > int64(rl.maxResponseSize) {
		return fmt.Errorf(ErrMaxResponseSizeHit, rl.maxResponseSize)
	}
	return nil
}
