package api

import (
	"encoding/json"
	"net/http"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

type Response struct {
	Status    string       `json:"status"`
	Data      interface{}  `json:"data"`
	ErrorType v1.ErrorType `json:"errorType"`
	Error     string       `json:"error"`
	Warnings  []string     `json:"warnings,omitempty"`
}

func RespondError(logger log.Logger, w http.ResponseWriter, errorType v1.ErrorType, msg string, statusCode int) {
	var (
		res Response
		b   []byte
		err error
	)
	b = yoloBuf(msg)
	// Try to deserialize response and see if it is already in Prometheus error format.
	if err := json.Unmarshal(b, &res); err != nil {
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

// yoloBuf will return an unsafe pointer to a string, as the name yolo.yoloBuf implies use at your own risk.
func yoloBuf(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}
