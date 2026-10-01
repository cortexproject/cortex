package push

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util"
	util_api "github.com/cortexproject/cortex/pkg/util/api"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const testOTLPGRPCTenant = "user-1"

// startOTLPGRPCServer starts an in-memory gRPC server with the OTLP receiver and the same
// tenant interceptor that Cortex uses when auth is enabled. It returns a real OTLP client.
func startOTLPGRPCServer(t *testing.T, srv *OTLPGRPCServer) pmetricotlp.GRPCClient {
	t.Helper()

	listen := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor))
	pmetricotlp.RegisterGRPCServer(server, srv)
	go func() { _ = server.Serve(listen) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listen.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return pmetricotlp.NewGRPCClient(conn)
}

func newTestOTLPGRPCServer(cfg distributor.OTLPConfig, sourceIPs *middleware.SourceIPExtractor, push Func, requestTotal *prometheus.CounterVec) *OTLPGRPCServer {
	overrides := validation.NewOverrides(querier.DefaultLimitsConfig(), nil)
	return NewOTLPGRPCServer(overrides, cfg, sourceIPs, push, requestTotal)
}

func tenantContext() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "x-scope-orgid", testOTLPGRPCTenant)
}

func newRequestTotal() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_requests_total"}, []string{"type"})
}

// exportRequestWithSums makes a request with one sum per given temporality.
func exportRequestWithSums(temporalities ...pmetric.AggregationTemporality) pmetricotlp.ExportRequest {
	md := pmetric.NewMetrics()
	metrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for i, temporality := range temporalities {
		createOtelSum(fmt.Sprintf("test_sum_%d", i), "", temporality, time.Now()).CopyTo(metrics.AppendEmpty())
	}
	return pmetricotlp.NewExportRequestFromMetrics(md)
}

func TestOTLPGRPCServer_Success(t *testing.T) {
	requestTotal := newRequestTotal()
	srv := newTestOTLPGRPCServer(distributor.OTLPConfig{}, nil, verifyOTLPWriteRequestHandler(t, cortexpb.API), requestTotal)
	client := startOTLPGRPCServer(t, srv)

	resp, err := client.Export(tenantContext(), generateOTLPWriteRequest())
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.PartialSuccess().RejectedDataPoints())
	assert.Empty(t, resp.PartialSuccess().ErrorMessage())

	assert.Equal(t, 1.0, testutil.ToFloat64(requestTotal.WithLabelValues(labelValueOTLPGRPC)))
	assert.Equal(t, 0.0, testutil.ToFloat64(requestTotal.WithLabelValues(labelValueOTLP)))
}

func TestOTLPGRPCServer_PushReceivesTenant(t *testing.T) {
	var gotTenant string
	push := func(ctx context.Context, _ *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
		gotTenant, _ = users.TenantID(ctx)
		return &cortexpb.WriteResponse{}, nil
	}
	client := startOTLPGRPCServer(t, newTestOTLPGRPCServer(distributor.OTLPConfig{}, nil, push, nil))

	_, err := client.Export(tenantContext(), generateOTLPWriteRequest())
	require.NoError(t, err)
	assert.Equal(t, testOTLPGRPCTenant, gotTenant)
}

func TestOTLPGRPCServer_MissingTenant(t *testing.T) {
	pushCalled := false
	push := func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
		pushCalled = true
		return &cortexpb.WriteResponse{}, nil
	}

	t.Run("rejected by the auth interceptor", func(t *testing.T) {
		client := startOTLPGRPCServer(t, newTestOTLPGRPCServer(distributor.OTLPConfig{}, nil, push, nil))
		_, err := client.Export(context.Background(), generateOTLPWriteRequest())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no org id")
	})

	t.Run("rejected by the server when no interceptor sets the tenant", func(t *testing.T) {
		srv := newTestOTLPGRPCServer(distributor.OTLPConfig{}, nil, push, nil)
		_, err := srv.Export(context.Background(), generateOTLPWriteRequest())
		require.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, grpcstatus.Code(err))
	})

	assert.False(t, pushCalled)
}

func TestOTLPGRPCServer_DeltaTemporality(t *testing.T) {
	tests := map[string]struct {
		allowDelta          bool
		temporalities       []pmetric.AggregationTemporality
		expectedCode        codes.Code
		expectPush          bool
		expectPartialErrMsg bool
	}{
		"delta not allowed, only delta: rejected": {
			temporalities: []pmetric.AggregationTemporality{pmetric.AggregationTemporalityDelta},
			expectedCode:  codes.InvalidArgument,
		},
		"delta not allowed, delta and cumulative: partial success": {
			temporalities:       []pmetric.AggregationTemporality{pmetric.AggregationTemporalityCumulative, pmetric.AggregationTemporalityDelta},
			expectedCode:        codes.OK,
			expectPush:          true,
			expectPartialErrMsg: true,
		},
		"delta allowed, delta and cumulative: success": {
			allowDelta:    true,
			temporalities: []pmetric.AggregationTemporality{pmetric.AggregationTemporalityCumulative, pmetric.AggregationTemporalityDelta},
			expectedCode:  codes.OK,
			expectPush:    true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pushCalled := false
			push := func(_ context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
				pushCalled = true
				assert.NotEmpty(t, req.Timeseries)
				return &cortexpb.WriteResponse{}, nil
			}
			cfg := distributor.OTLPConfig{AllowDeltaTemporality: tc.allowDelta}
			client := startOTLPGRPCServer(t, newTestOTLPGRPCServer(cfg, nil, push, nil))

			resp, err := client.Export(tenantContext(), exportRequestWithSums(tc.temporalities...))
			assert.Equal(t, tc.expectedCode, grpcstatus.Code(err))
			assert.Equal(t, tc.expectPush, pushCalled)
			if err != nil {
				return
			}

			// The converter does not count dropped data points, so this must always be 0.
			assert.Equal(t, int64(0), resp.PartialSuccess().RejectedDataPoints())
			if tc.expectPartialErrMsg {
				assert.Contains(t, resp.PartialSuccess().ErrorMessage(), "invalid temporality and type combination")
			} else {
				assert.Empty(t, resp.PartialSuccess().ErrorMessage())
			}
		})
	}
}

func TestOTLPGRPCServer_PushErrors(t *testing.T) {
	tests := map[string]struct {
		pushErr          error
		expectedCode     codes.Code
		expectedHTTPCode int32
	}{
		"HA dedup (202) is a success": {
			pushErr:      httpgrpc.Errorf(http.StatusAccepted, "deduplicated"),
			expectedCode: codes.OK,
		},
		"400 is not retryable": {
			pushErr:          httpgrpc.Errorf(http.StatusBadRequest, "bad labels"),
			expectedCode:     codes.InvalidArgument,
			expectedHTTPCode: http.StatusBadRequest,
		},
		"413 is not retryable": {
			pushErr:          httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "too large"),
			expectedCode:     codes.InvalidArgument,
			expectedHTTPCode: http.StatusRequestEntityTooLarge,
		},
		"429 is retryable": {
			pushErr:          httpgrpc.Errorf(http.StatusTooManyRequests, "rate limited"),
			expectedCode:     codes.Unavailable,
			expectedHTTPCode: http.StatusTooManyRequests,
		},
		"503 is retryable": {
			pushErr:          httpgrpc.Errorf(http.StatusServiceUnavailable, "too many inflight"),
			expectedCode:     codes.Unavailable,
			expectedHTTPCode: http.StatusServiceUnavailable,
		},
		"500 is retryable": {
			pushErr:          httpgrpc.Errorf(http.StatusInternalServerError, "ingester failed"),
			expectedCode:     codes.Unavailable,
			expectedHTTPCode: http.StatusInternalServerError,
		},
		"plain error is retryable": {
			pushErr:          errors.New("too many unhealthy instances in the ring"),
			expectedCode:     codes.Unavailable,
			expectedHTTPCode: http.StatusInternalServerError,
		},
		"context canceled": {
			pushErr:          context.Canceled,
			expectedCode:     codes.Canceled,
			expectedHTTPCode: util_api.StatusClientClosedRequest,
		},
		"wrapped context canceled": {
			pushErr:          fmt.Errorf("push failed: %w", context.Canceled),
			expectedCode:     codes.Canceled,
			expectedHTTPCode: util_api.StatusClientClosedRequest,
		},
		"deadline exceeded": {
			pushErr:          context.DeadlineExceeded,
			expectedCode:     codes.DeadlineExceeded,
			expectedHTTPCode: http.StatusGatewayTimeout,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			push := func(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
				return nil, tc.pushErr
			}
			client := startOTLPGRPCServer(t, newTestOTLPGRPCServer(distributor.OTLPConfig{}, nil, push, nil))

			_, err := client.Export(tenantContext(), generateOTLPWriteRequest())
			require.Equal(t, tc.expectedCode, grpcstatus.Code(err))
			if tc.expectedCode == codes.OK {
				return
			}

			// The HTTP code must stay in the error detail, because the gRPC server
			// instrumentation uses it for the status_code label.
			httpResp, ok := httpgrpc.HTTPResponseFromError(err)
			require.True(t, ok)
			assert.Equal(t, tc.expectedHTTPCode, httpResp.Code)
		})
	}
}

func TestOTLPGRPCServer_SourceIPs(t *testing.T) {
	sourceIPs, err := middleware.NewSourceIPs("", "")
	require.NoError(t, err)

	var gotSource string
	push := func(ctx context.Context, _ *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
		gotSource = util.GetSourceIPsFromOutgoingCtx(ctx)
		return &cortexpb.WriteResponse{}, nil
	}
	client := startOTLPGRPCServer(t, newTestOTLPGRPCServer(distributor.OTLPConfig{}, sourceIPs, push, nil))

	ctx := metadata.AppendToOutgoingContext(tenantContext(), "x-forwarded-for", "1.2.3.4")
	_, err = client.Export(ctx, generateOTLPWriteRequest())
	require.NoError(t, err)
	assert.Contains(t, gotSource, "1.2.3.4")
}

func TestOTLPGRPCCode(t *testing.T) {
	tests := map[int]codes.Code{
		http.StatusBadRequest:              codes.InvalidArgument,
		http.StatusUnauthorized:            codes.Unauthenticated,
		http.StatusForbidden:               codes.PermissionDenied,
		http.StatusRequestEntityTooLarge:   codes.InvalidArgument,
		http.StatusTooManyRequests:         codes.Unavailable,
		util_api.StatusClientClosedRequest: codes.Canceled,
		http.StatusInternalServerError:     codes.Unavailable,
		http.StatusServiceUnavailable:      codes.Unavailable,
		http.StatusGatewayTimeout:          codes.DeadlineExceeded,
	}
	for httpCode, expected := range tests {
		assert.Equal(t, expected, otlpGRPCCode(httpCode), "HTTP %d", httpCode)
	}
}

func TestTruncate(t *testing.T) {
	assert.Equal(t, "abc", truncate("abc", 5))
	assert.Equal(t, "ab", truncate("abc", 2))
}
