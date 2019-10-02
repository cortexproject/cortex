package frontend

import (
	"bytes"
	fmt "fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
)

func TestRetry(t *testing.T) {
	var try int32
	for _, tc := range []struct {
		name         string
		roundtripper http.RoundTripper
		resp         *http.Response
		err          error
	}{
		{
			name: "retry failures",
			roundtripper: RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if atomic.AddInt32(&try, 1) == 5 {
					return &http.Response{
						StatusCode: 200,
						Body:       ioutil.NopCloser(bytes.NewBufferString(`Hello World`)),
						Header:     make(http.Header),
					}, nil
				}
				return nil, fmt.Errorf("fail")
			}),
			resp: &http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(bytes.NewBufferString(`Hello World`)),
				Header:     make(http.Header),
			},
		},
		{
			name: "don't retry 400s",
			roundtripper: RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, httpgrpc.Errorf(http.StatusBadRequest, "Bad Request")
			}),
			err: httpgrpc.Errorf(http.StatusBadRequest, "Bad Request"),
		},
		{
			name: "retry 500s",
			roundtripper: RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, httpgrpc.Errorf(http.StatusInternalServerError, "Internal Server Error")
			}),
			err: httpgrpc.Errorf(http.StatusInternalServerError, "Internal Server Error"),
		},
		{
			name: "last error",
			roundtripper: RoundTripFunc(func(req *http.Request) (*http.Response, error) {
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
			h := NewRetryTripperware(log.NewNopLogger(), 5)(tc.roundtripper)
			resp, err := h.RoundTrip(httptest.NewRequest("", "/test", nil))
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.resp, resp)
		})
	}
}
