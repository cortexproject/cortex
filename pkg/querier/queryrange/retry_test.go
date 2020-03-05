package queryrange

import (
	"context"
	"errors"
	fmt "fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
)

func TestRetry(t *testing.T) {
	var try int32

	for _, tc := range []struct {
		name    string
		handler Handler
		resp    Response
		err     error
	}{
		{
			name: "retry failures",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				if atomic.AddInt32(&try, 1) == 5 {
					return &PrometheusResponse{Status: "Hello World"}, nil
				}
				return nil, fmt.Errorf("fail")
			}),
			resp: &PrometheusResponse{Status: "Hello World"},
		},
		{
			name: "don't retry 400s",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, httpgrpc.Errorf(http.StatusBadRequest, "Bad Request")
			}),
			err: httpgrpc.Errorf(http.StatusBadRequest, "Bad Request"),
		},
		{
			name: "retry 500s",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, httpgrpc.Errorf(http.StatusInternalServerError, "Internal Server Error")
			}),
			err: httpgrpc.Errorf(http.StatusInternalServerError, "Internal Server Error"),
		},
		{
			name: "last error",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				if atomic.AddInt32(&try, 1) == 5 {
					return nil, httpgrpc.Errorf(http.StatusBadRequest, "Bad Request")
				}
				return nil, httpgrpc.Errorf(http.StatusInternalServerError, "Internal Server Error")
			}),
			err: httpgrpc.Errorf(http.StatusBadRequest, "Bad Request"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			try = 0
			h := NewRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(tc.handler)
			resp, err := h.Do(context.Background(), nil)
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.resp, resp)
		})
	}
}

func Test_RetryMiddlewareCancel(t *testing.T) {
	var try int32
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(c context.Context, r Request) (Response, error) {
			atomic.AddInt32(&try, 1)
			return nil, ctx.Err()
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(0), try)
	require.Equal(t, ctx.Err(), err)

	ctx, cancel = context.WithCancel(context.Background())
	_, err = NewRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(c context.Context, r Request) (Response, error) {
			atomic.AddInt32(&try, 1)
			cancel()
			return nil, errors.New("failed")
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(1), try)
	require.Equal(t, ctx.Err(), err)
}
