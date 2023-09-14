package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"
)

func TestRetry(t *testing.T) {
	tries := atomic.NewInt64(3)
	r := NewRetry(3, nil)

	res, err := r.Do(func() (*httpgrpc.HTTPResponse, error) {
		try := tries.Dec()
		if try > 1 {
			return &httpgrpc.HTTPResponse{
				Code: 500,
			}, nil
		} else {
			return &httpgrpc.HTTPResponse{
				Code: 200,
			}, nil
		}
	})

	require.NoError(t, err)
	require.Equal(t, int32(200), res.Code)
}
