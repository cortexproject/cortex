package push

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/cortexpbv2"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/log"
)

const (
	remoteWriteVersionHeader        = "X-Prometheus-Remote-Write-Version"
	remoteWriteVersion1HeaderValue  = "0.1.0"
	remoteWriteVersion20HeaderValue = "2.0.0"
	appProtoContentType             = "application/x-protobuf"
	appProtoV1ContentType           = "application/x-protobuf;proto=prometheus.WriteRequest"
	appProtoV2ContentType           = "application/x-protobuf;proto=io.prometheus.write.v2.Request"

	rw20WrittenSamplesHeader    = "X-Prometheus-Remote-Write-Samples-Written"
	rw20WrittenHistogramsHeader = "X-Prometheus-Remote-Write-Histograms-Written"
	rw20WrittenExemplarsHeader  = "X-Prometheus-Remote-Write-Exemplars-Written"
)

// Func defines the type of the push. It is similar to http.HandlerFunc.
type Func func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error)

// FuncV2 defines the type of the pushV2. It is similar to http.HandlerFunc.
type FuncV2 func(ctx context.Context, request *cortexpbv2.WriteRequest) (*cortexpbv2.WriteResponse, error)

// Handler is a http.Handler which accepts WriteRequests.
func Handler(maxRecvMsgSize int, sourceIPs *middleware.SourceIPExtractor, push Func, pushV2 FuncV2) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := log.WithContext(ctx, log.Logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
				logger = log.WithSourceIPs(source, logger)
			}
		}

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = appProtoContentType
		}

		msgType, err := parseProtoMsg(contentType)
		if err != nil {
			level.Error(logger).Log("Error decoding remote write request", "err", err)
			http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
			return
		}

		if msgType != config.RemoteWriteProtoMsgV1 && msgType != config.RemoteWriteProtoMsgV2 {
			level.Error(logger).Log("Error decoding remote write request", "err", err)
			http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
			return
		}

		enc := r.Header.Get("Content-Encoding")
		if enc == "" {
		} else if enc != string(remote.SnappyBlockCompression) {
			err := fmt.Errorf("%v encoding (compression) is not accepted by this server; only %v is acceptable", enc, remote.SnappyBlockCompression)
			level.Error(logger).Log("Error decoding remote write request", "err", err)
			http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
			return
		}

		switch msgType {
		case config.RemoteWriteProtoMsgV1:
			var req cortexpb.PreallocWriteRequest
			err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, &req, util.RawSnappy)
			if err != nil {
				level.Error(logger).Log("err", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			req.SkipLabelNameValidation = false
			if req.Source == 0 {
				req.Source = cortexpb.API
			}

			if _, err := push(ctx, &req.WriteRequest); err != nil {
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				if !ok {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if resp.GetCode()/100 == 5 {
					level.Error(logger).Log("msg", "push error", "err", err)
				} else if resp.GetCode() != http.StatusAccepted && resp.GetCode() != http.StatusTooManyRequests {
					level.Warn(logger).Log("msg", "push refused", "err", err)
				}
				http.Error(w, string(resp.Body), int(resp.Code))
			}
		case config.RemoteWriteProtoMsgV2:
			var req cortexpbv2.WriteRequest
			err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, &req, util.RawSnappy)
			if err != nil {
				fmt.Println("err", err)
				level.Error(logger).Log("err", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			req.SkipLabelNameValidation = false
			if req.Source == 0 {
				req.Source = cortexpbv2.API
			}

			if resp, err := pushV2(ctx, &req); err != nil {
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				w.Header().Set(rw20WrittenSamplesHeader, "0")
				w.Header().Set(rw20WrittenHistogramsHeader, "0")
				w.Header().Set(rw20WrittenExemplarsHeader, "0")
				if !ok {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if resp.GetCode()/100 == 5 {
					level.Error(logger).Log("msg", "push error", "err", err)
				} else if resp.GetCode() != http.StatusAccepted && resp.GetCode() != http.StatusTooManyRequests {
					level.Warn(logger).Log("msg", "push refused", "err", err)
				}
				http.Error(w, string(resp.Body), int(resp.Code))
			} else {
				w.Header().Set(rw20WrittenSamplesHeader, strconv.FormatInt(resp.Samples, 10))
				w.Header().Set(rw20WrittenHistogramsHeader, strconv.FormatInt(resp.Histograms, 10))
				w.Header().Set(rw20WrittenExemplarsHeader, strconv.FormatInt(resp.Exemplars, 10))
			}
		}
	})
}

// Refer to parseProtoMsg in https://github.com/prometheus/prometheus/blob/main/storage/remote/write_handler.go
func parseProtoMsg(contentType string) (config.RemoteWriteProtoMsg, error) {
	contentType = strings.TrimSpace(contentType)

	parts := strings.Split(contentType, ";")
	if parts[0] != appProtoContentType {
		return "", fmt.Errorf("expected %v as the first (media) part, got %v content-type", appProtoContentType, contentType)
	}
	// Parse potential https://www.rfc-editor.org/rfc/rfc9110#parameter
	for _, p := range parts[1:] {
		pair := strings.Split(p, "=")
		if len(pair) != 2 {
			return "", fmt.Errorf("as per https://www.rfc-editor.org/rfc/rfc9110#parameter expected parameters to be key-values, got %v in %v content-type", p, contentType)
		}
		if pair[0] == "proto" {
			ret := config.RemoteWriteProtoMsg(pair[1])
			if err := ret.Validate(); err != nil {
				return "", fmt.Errorf("got %v content type; %w", contentType, err)
			}
			return ret, nil
		}
	}
	// No "proto=" parameter, assuming v1.
	return config.RemoteWriteProtoMsgV1, nil
}
