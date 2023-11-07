package transport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"
)

func TestRetry(t *testing.T) {
	tries := atomic.NewInt64(3)
	r := NewRetry(3, nil)
	ctx := context.Background()
	res, err := r.Do(ctx, func() (*httpgrpc.HTTPResponse, error) {
		try := tries.Dec()
		if try > 1 {
			return &httpgrpc.HTTPResponse{
				Code: 500,
			}, nil
		}
		return &httpgrpc.HTTPResponse{
			Code: 200,
		}, nil

	})

	require.NoError(t, err)
	require.Equal(t, int32(200), res.Code)
}

func TestNoRetryOnChunkPoolExhaustion(t *testing.T) {
	tries := atomic.NewInt64(3)
	r := NewRetry(3, nil)
	ctx := context.Background()
	res, err := r.Do(ctx, func() (*httpgrpc.HTTPResponse, error) {
		try := tries.Dec()
		if try > 1 {
			return &httpgrpc.HTTPResponse{
				Code: 500,
				Body: []byte(pool.ErrPoolExhausted.Error()),
			}, nil
		}
		return &httpgrpc.HTTPResponse{
			Code: 200,
		}, nil

	})

	require.NoError(t, err)
	require.Equal(t, int32(500), res.Code)
}
