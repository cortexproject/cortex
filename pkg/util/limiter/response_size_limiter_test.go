package limiter

import (
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
