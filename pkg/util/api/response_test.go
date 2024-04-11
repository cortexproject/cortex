package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/codes"
)

func TestRespondFromGRPCError(t *testing.T) {
	logger := log.NewNopLogger()
	for _, tc := range []struct {
		name         string
		err          error
		expectedResp *Response
		code         int
	}{
		{
			name: "non grpc error",
			err:  errors.New("test"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrServer,
				Error:     "test",
			},
			code: 500,
		},
		{
			name: "bad data",
			err:  httpgrpc.Errorf(http.StatusBadRequest, "bad_data"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrBadData,
				Error:     "bad_data",
			},
			code: http.StatusBadRequest,
		},
		{
			name: "413",
			err:  httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "bad_data"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrBadData,
				Error:     "bad_data",
			},
			code: http.StatusRequestEntityTooLarge,
		},
		{
			name: "499",
			err:  httpgrpc.Errorf(StatusClientClosedRequest, "bad_data"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrCanceled,
				Error:     "bad_data",
			},
			code: StatusClientClosedRequest,
		},
		{
			name: "504",
			err:  httpgrpc.Errorf(http.StatusGatewayTimeout, "bad_data"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrTimeout,
				Error:     "bad_data",
			},
			code: http.StatusGatewayTimeout,
		},
		{
			name: "422",
			err:  httpgrpc.Errorf(http.StatusUnprocessableEntity, "bad_data"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrExec,
				Error:     "bad_data",
			},
			code: http.StatusUnprocessableEntity,
		},
		{
			name: "grpc status code",
			err:  httpgrpc.Errorf(int(codes.PermissionDenied), "bad_data"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrBadData,
				Error:     "bad_data",
			},
			code: http.StatusUnprocessableEntity,
		},
		{
			name: "other status code defaults to err server",
			err:  httpgrpc.Errorf(http.StatusTooManyRequests, "bad_data"),
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrServer,
				Error:     "bad_data",
			},
			code: http.StatusTooManyRequests,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := httptest.NewRecorder()
			RespondFromGRPCError(logger, writer, tc.err)
			output, err := io.ReadAll(writer.Body)
			require.NoError(t, err)
			var res Response
			err = json.Unmarshal(output, &res)
			require.NoError(t, err)

			require.Equal(t, tc.expectedResp.Status, res.Status)
			require.Equal(t, tc.expectedResp.Error, res.Error)
			require.Equal(t, tc.expectedResp.ErrorType, res.ErrorType)

			require.Equal(t, tc.code, writer.Code)
		})
	}
}

func TestRespondError(t *testing.T) {
	logger := log.NewNopLogger()
	for _, tc := range []struct {
		name         string
		errorType    v1.ErrorType
		msg          string
		status       string
		code         int
		expectedResp *Response
	}{
		{
			name:      "bad data",
			errorType: v1.ErrBadData,
			msg:       "test_msg",
			status:    "error",
			code:      400,
		},
		{
			name:      "server error",
			errorType: v1.ErrServer,
			msg:       "test_msg",
			status:    "error",
			code:      500,
		},
		{
			name:      "canceled",
			errorType: v1.ErrCanceled,
			msg:       "test_msg",
			status:    "error",
			code:      499,
		},
		{
			name:      "timeout",
			errorType: v1.ErrTimeout,
			msg:       "test_msg",
			status:    "error",
			code:      502,
		},
		{
			name: "prometheus_format_error",
			expectedResp: &Response{
				Status:    "error",
				ErrorType: v1.ErrServer,
				Error:     "server_error",
			},
			code:      400,
			status:    "error",
			errorType: v1.ErrBadData,
		},
		{
			// If the input Prometheus error cannot be unmarshalled,
			// use the error type and message provided in the function.
			name:      "bad_prometheus_format_error",
			msg:       `"status":"error","data":null,"errorType":"bad_data","error":"bad_data"}`,
			code:      500,
			status:    "error",
			errorType: v1.ErrServer,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.msg
			if tc.expectedResp != nil {
				output, err := json.Marshal(tc.expectedResp)
				require.NoError(t, err)
				msg = string(output)
			}
			writer := httptest.NewRecorder()
			RespondError(logger, writer, tc.errorType, msg, tc.code)
			output, err := io.ReadAll(writer.Body)
			require.NoError(t, err)
			var res Response
			err = json.Unmarshal(output, &res)
			require.NoError(t, err)

			if tc.expectedResp == nil {
				require.Equal(t, tc.status, res.Status)
				require.Equal(t, tc.msg, res.Error)
				require.Equal(t, tc.errorType, res.ErrorType)
			} else {
				require.Equal(t, tc.expectedResp.Status, res.Status)
				require.Equal(t, tc.expectedResp.Error, res.Error)
				require.Equal(t, tc.expectedResp.ErrorType, res.ErrorType)
			}

			require.Equal(t, tc.code, writer.Code)
		})
	}
}
