package httpgrpcutil

import (
	"fmt"
	"net/http"

	"github.com/weaveworks/common/httpgrpc"
)

func WrapHTTPGrpcError(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	msg := fmt.Sprintf(format, args...)
	resp, ok := httpgrpc.HTTPResponseFromError(err)
	if !ok {
		return httpgrpc.Errorf(http.StatusInternalServerError, "%s, %s", msg, err)
	}
	return httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code:    resp.Code,
		Headers: resp.Headers,
		Body:    []byte(fmt.Sprintf("%s, %s", msg, err)),
	})
}
