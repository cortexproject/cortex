package push

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/util"
	util_api "github.com/cortexproject/cortex/pkg/util/api"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// maxPartialSuccessMessageLen limits the size of the partial success message. The
// conversion error joins one error per dropped metric, so it can be very long.
const maxPartialSuccessMessageLen = 1024

// OTLPGRPCServer receives OTLP metrics over gRPC, using the standard
// opentelemetry.proto.collector.metrics.v1.MetricsService/Export method.
type OTLPGRPCServer struct {
	pmetricotlp.UnimplementedGRPCServer

	overrides    *validation.Overrides
	cfg          distributor.OTLPConfig
	sourceIPs    *middleware.SourceIPExtractor
	push         Func
	requestTotal *prometheus.CounterVec
}

// NewOTLPGRPCServer makes a new OTLP gRPC server. Register it with pmetricotlp.RegisterGRPCServer.
func NewOTLPGRPCServer(overrides *validation.Overrides, cfg distributor.OTLPConfig, sourceIPs *middleware.SourceIPExtractor, push Func, requestTotal *prometheus.CounterVec) *OTLPGRPCServer {
	return &OTLPGRPCServer{
		overrides:    overrides,
		cfg:          cfg,
		sourceIPs:    sourceIPs,
		push:         push,
		requestTotal: requestTotal,
	}
}

// Export implements pmetricotlp.GRPCServer.
func (s *OTLPGRPCServer) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	resp := pmetricotlp.NewExportResponse()

	logger := util_log.WithContext(ctx, util_log.Logger)
	if s.sourceIPs != nil {
		source := s.sourceIPs.Get(httpRequestFromGRPCContext(ctx))
		if source != "" {
			ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
			logger = util_log.WithSourceIPs(source, logger)
		}
	}

	userID, err := users.TenantID(ctx)
	if err != nil {
		if errors.Is(err, user.ErrNoOrgID) {
			return resp, status.Error(codes.Unauthenticated, err.Error())
		}
		return resp, status.Error(codes.InvalidArgument, err.Error())
	}

	if s.requestTotal != nil {
		s.requestTotal.WithLabelValues(labelValueOTLPGRPC).Inc()
	}

	prwReq, convErr := convertOTLPToWriteRequest(ctx, req.Metrics(), s.cfg, s.overrides, userID, logger)
	// The conversion stops early when the context is done. In that case the error does
	// not describe dropped metrics, so it must not become a partial success.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return resp, toOTLPGRPCError(ctxErr, logger)
	}
	if convErr != nil && len(prwReq.Timeseries) == 0 {
		return resp, status.Error(codes.InvalidArgument, convErr.Error())
	}

	if _, err := s.push(ctx, prwReq); err != nil {
		if grpcErr := toOTLPGRPCError(err, logger); grpcErr != nil {
			return resp, grpcErr
		}
	}

	if convErr != nil {
		// RejectedDataPoints stays 0. The converter does not count the dropped data
		// points, and the converted series do not map 1:1 to OTLP data points, so no
		// correct count is available.
		resp.PartialSuccess().SetErrorMessage(truncate(convErr.Error(), maxPartialSuccessMessageLen))
	}

	return resp, nil
}

// toOTLPGRPCError converts an error from the distributor push into a gRPC error with a
// status code that OTLP clients use to decide if they must retry. It returns nil when the
// push must be reported as successful, for example when the HA tracker deduplicated it.
//
// The returned error keeps the original httpgrpc.HTTPResponse as its only detail, so the
// gRPC server instrumentation still records the HTTP status code (for example "429").
func toOTLPGRPCError(err error, logger log.Logger) error {
	switch {
	case errors.Is(err, context.Canceled):
		err = httpgrpc.Errorf(util_api.StatusClientClosedRequest, "%s", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		err = httpgrpc.Errorf(http.StatusGatewayTimeout, "%s", err.Error())
	}

	httpResp, ok := httpgrpc.HTTPResponseFromError(err)
	if !ok {
		httpResp = &httpgrpc.HTTPResponse{Code: http.StatusInternalServerError, Body: []byte(err.Error())}
	}

	httpCode := int(httpResp.GetCode())
	if httpCode/100 == 2 {
		return nil
	}

	if httpCode/100 == 5 {
		level.Error(logger).Log("msg", "push error", "err", err)
	} else if httpCode != http.StatusTooManyRequests && httpCode != util_api.StatusClientClosedRequest {
		level.Warn(logger).Log("msg", "push refused", "err", err)
	}

	st := status.New(otlpGRPCCode(httpCode), string(httpResp.Body))
	if withDetail, detailErr := st.WithDetails(httpResp); detailErr == nil {
		st = withDetail
	}
	return st.Err()
}

// otlpGRPCCode maps an HTTP status code to the gRPC code that gives the retry behavior
// the OTLP specification requires. A 429 maps to Unavailable, because ResourceExhausted
// without RetryInfo is a permanent error for OTLP clients.
func otlpGRPCCode(httpCode int) codes.Code {
	switch {
	case httpCode == http.StatusUnauthorized:
		return codes.Unauthenticated
	case httpCode == http.StatusForbidden:
		return codes.PermissionDenied
	case httpCode == http.StatusTooManyRequests:
		return codes.Unavailable
	case httpCode == util_api.StatusClientClosedRequest:
		return codes.Canceled
	case httpCode == http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	case httpCode/100 == 4:
		return codes.InvalidArgument
	default:
		return codes.Unavailable
	}
}

// httpRequestFromGRPCContext makes an HTTP request that holds the incoming gRPC
// metadata as headers and the peer address as RemoteAddr. It lets the gRPC path use
// the same middleware.SourceIPExtractor as the HTTP path.
func httpRequestFromGRPCContext(ctx context.Context) *http.Request {
	r := &http.Request{Header: http.Header{}}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for k, values := range md {
			for _, v := range values {
				r.Header.Add(k, v)
			}
		}
	}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		r.RemoteAddr = p.Addr.String()
	}
	return r
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
