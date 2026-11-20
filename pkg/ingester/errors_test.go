package ingester

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
)

func TestUnloggedError(t *testing.T) {
	const msg = `user=test: err: duplicate sample for timestamp. series={__name__="foo"}`
	pushErr := httpgrpc.Errorf(http.StatusBadRequest, "%s", msg)

	// serverLogOutput runs err through the gRPC server logging interceptor and returns what it logged.
	serverLogOutput := func(err error) string {
		var buf bytes.Buffer
		serverLog := middleware.GRPCServerLog{Log: logging.GoKit(log.NewLogfmtLogger(&buf))}
		handler := func(context.Context, any) (any, error) { return nil, err }
		_, got := serverLog.UnaryServerInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/cortex.Ingester/Push"}, handler)
		require.Equal(t, err, got)
		return buf.String()
	}

	t.Run("the gRPC server does not log it", func(t *testing.T) {
		assert.Empty(t, serverLogOutput(unloggedError{pushErr}))
	})

	t.Run("a plain error is still logged", func(t *testing.T) {
		assert.Contains(t, serverLogOutput(pushErr), "duplicate sample for timestamp")
	})

	t.Run("the distributor still receives the same 400", func(t *testing.T) {
		err := unloggedError{pushErr}
		assert.Equal(t, pushErr.Error(), err.Error())

		resp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok)
		assert.Equal(t, int32(http.StatusBadRequest), resp.Code)
		assert.Equal(t, msg, string(resp.Body))
	})
}
