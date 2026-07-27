package ingester

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// pushStreamServer is a mock implementation of Ingester_PushStreamServer for testing PushStream.
// Context() injects stream-level authentication (X-Scope-OrgID),
// and Recv() provides the sequence of StreamWriteRequests sent by the client.
type pushStreamServer struct {
	grpc.ServerStream
	ctx      context.Context
	requests []*cortexpb.StreamWriteRequest
	idx      int
	sent     []*cortexpb.WriteResponse
}

func (s *pushStreamServer) Context() context.Context { return s.ctx }

func (s *pushStreamServer) Recv() (*cortexpb.StreamWriteRequest, error) {
	if s.idx >= len(s.requests) {
		return nil, io.EOF
	}
	req := s.requests[s.idx]
	s.idx++
	return req, nil
}

func (s *pushStreamServer) Send(resp *cortexpb.WriteResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

// makeWriteReq builds a WriteRequest containing a single sample with the given metric name.
func makeWriteReq(metricName string) *cortexpb.WriteRequest {
	return &cortexpb.WriteRequest{
		Timeseries: []cortexpb.PreallocTimeseries{
			{
				TimeSeries: &cortexpb.TimeSeries{
					Labels: []cortexpb.LabelAdapter{
						{Name: "__name__", Value: metricName},
					},
					Samples: []cortexpb.Sample{
						{Value: 13.37, TimestampMs: time.Now().UnixMilli()},
					},
				},
			},
		},
	}
}

// newTestIngester creates an ingester instance for PushStream security tests.
func newTestIngester(t *testing.T) *Ingester {
	t.Helper()
	cfg := defaultIngesterTestConfig(t)
	ing, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), ing)
	})
	test.Poll(t, time.Second, ring.ACTIVE, func() any {
		return ing.lifecycler.GetState()
	})
	return ing
}

// TestPushStream_TenantImpersonation reproduces the tenant impersonation vulnerability
// and verifies it is blocked after the fix is applied.
func TestPushStream_TenantImpersonation(t *testing.T) {
	ing := newTestIngester(t)

	// The stream is authenticated as user-1 (X-Scope-OrgID: user-1).
	streamCtx := user.InjectOrgID(context.Background(), "user-1")

	srv := &pushStreamServer{
		ctx: streamCtx,
		requests: []*cortexpb.StreamWriteRequest{
			{
				// Impersonation: stream is user-1 but the request claims user-2.
				TenantID: "user-2",
				Request:  makeWriteReq("poc_injected_metric"),
			},
		},
	}

	err := ing.PushStream(srv)

	require.Error(t, err, "a stream authenticated as user-1 must not write for user-2")
	st, ok := status.FromError(err)
	require.True(t, ok, "error must be a gRPC status error")
	require.Equal(t, codes.PermissionDenied, st.Code(),
		"tenant ID mismatch must result in PermissionDenied")
}

// TestAttack_DirectGRPC_BypassWithWorkerID proves that a knowledgeable attacker
// can bypass the tenant check by spoofing the distributor worker ID pattern.
func TestAttack_DirectGRPC_BypassWithWorkerID(t *testing.T) {
	ing := newTestIngester(t)

	// "ingester-<anything>-stream-push-worker-<any_number>"
	spoofedWorkerCtx := user.InjectOrgID(context.Background(), "ingester-fake-stream-push-worker-0")

	// Step 2: Attacker requests to write into "tenant-B"
	srv := &pushStreamServer{
		ctx: spoofedWorkerCtx,
		requests: []*cortexpb.StreamWriteRequest{
			{
				TenantID: "tenant-B",
				Request:  makeWriteReq("hacked_metric_bypass"),
			},
		},
	}

	err := ing.PushStream(srv)

	require.NoError(t, err, "ATTACK SUCCEEDED: The attacker bypassed the check by spoofing the worker ID, `-distributor.sign-write-requests-keys` should be used to prevent this")
}
