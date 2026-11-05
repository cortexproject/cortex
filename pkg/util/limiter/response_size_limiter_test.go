package limiter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseSizeLimiter_AddDataBytes(t *testing.T) {
	var responseSizeLimiter = NewResponseSizeLimiter(4096)

	err := responseSizeLimiter.AddResponseBytes(2048)
	require.NoError(t, err)
	err = responseSizeLimiter.AddResponseBytes(2048)
	require.NoError(t, err)
	err = responseSizeLimiter.AddResponseBytes(1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "the query response size exceeds limit")
}

func TestResponseSizeLimiterFromContextWithFallback(t *testing.T) {
	t.Run("missing limiter allows unlimited response bytes", func(t *testing.T) {
		limiter := ResponseSizeLimiterFromContextWithFallback(context.Background())
		require.NotNil(t, limiter)

		require.NoError(t, limiter.AddResponseBytes(4096))
		require.NoError(t, limiter.AddResponseBytes(4096))
	})

	t.Run("child contexts share the query response budget", func(t *testing.T) {
		limiter := NewResponseSizeLimiter(4096)
		ctx := AddResponseSizeLimiterToContext(context.Background(), limiter)
		firstCtx, cancelFirst := context.WithCancel(ctx)
		defer cancelFirst()
		secondCtx, cancelSecond := context.WithCancel(ctx)
		defer cancelSecond()

		first := ResponseSizeLimiterFromContextWithFallback(firstCtx)
		second := ResponseSizeLimiterFromContextWithFallback(secondCtx)
		require.Same(t, limiter, first)
		require.Same(t, limiter, second)

		require.NoError(t, first.AddResponseBytes(2048))
		require.NoError(t, second.AddResponseBytes(2048))
		require.EqualError(t, first.AddResponseBytes(1), "the query response size exceeds limit (limit: 4096 bytes)")
		require.EqualError(t, second.AddResponseBytes(1), "the query response size exceeds limit (limit: 4096 bytes)")
	})

	t.Run("separate queries have independent response budgets", func(t *testing.T) {
		parent := context.Background()
		firstCtx := AddResponseSizeLimiterToContext(parent, NewResponseSizeLimiter(4096))
		secondCtx := AddResponseSizeLimiterToContext(parent, NewResponseSizeLimiter(4096))
		first := ResponseSizeLimiterFromContextWithFallback(firstCtx)
		second := ResponseSizeLimiterFromContextWithFallback(secondCtx)

		require.NoError(t, first.AddResponseBytes(4096))
		require.EqualError(t, first.AddResponseBytes(1), "the query response size exceeds limit (limit: 4096 bytes)")
		require.NoError(t, second.AddResponseBytes(4096))
		require.NoError(t, ResponseSizeLimiterFromContextWithFallback(parent).AddResponseBytes(8192))
	})
}
