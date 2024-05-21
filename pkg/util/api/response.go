package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/codes"
)

const (
	// StatusClientClosedRequest is the status code for when a client request cancellation of a http request
	StatusClientClosedRequest = 499
)

// Response defines the Prometheus response format.
type Response struct {
	Status    string       `json:"status"`
	Data      interface{}  `json:"data,omitempty"`
	ErrorType v1.ErrorType `json:"errorType,omitempty"`
	Error     string       `json:"error,omitempty"`
	Warnings  []string     `json:"warnings,omitempty"`
}

// RespondFromGRPCError writes gRPC error in Prometheus response format.
// If error is not a valid gRPC error, use server_error instead.
func RespondFromGRPCError(logger log.Logger, w http.ResponseWriter, err error) {
	resp, ok := httpgrpc.HTTPResponseFromError(err)
	if ok {
		code := int(resp.Code)
		var errTyp v1.ErrorType
		switch resp.Code {
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
			errTyp = v1.ErrBadData
		case StatusClientClosedRequest:
			errTyp = v1.ErrCanceled
		case http.StatusGatewayTimeout:
			errTyp = v1.ErrTimeout
		case http.StatusUnprocessableEntity:
			errTyp = v1.ErrExec
		case int32(codes.PermissionDenied):
			// Convert gRPC status code to HTTP status code.
			code = http.StatusUnprocessableEntity
			errTyp = v1.ErrBadData
		default:
			errTyp = v1.ErrServer
		}
		RespondError(logger, w, errTyp, string(resp.Body), code)
	} else {
		RespondError(logger, w, v1.ErrServer, err.Error(), http.StatusInternalServerError)
	}
}

// RespondError writes error in Prometheus response format using provided error type and message.
func RespondError(logger log.Logger, w http.ResponseWriter, errorType v1.ErrorType, msg string, statusCode int) {
	var (
		res Response
		b   []byte
		err error
	)
	b = []byte(msg)
	// Try to deserialize response and see if it is already in Prometheus error format.
	if err = json.Unmarshal(b, &res); err != nil {
		b, err = json.Marshal(&Response{
			Status:    "error",
			ErrorType: errorType,
			Error:     msg,
			Data:      nil,
		})
	}

	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}
