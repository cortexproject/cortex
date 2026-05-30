package querier

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
)

// seriesStreamLifetimeClient is a BlocksStoreClient whose Series implementation is
// supplied by the test, so it can capture the per-stream context passed to Series and
// control the returned stream. LabelNames/LabelValues are inherited from the embedded
// storeGatewayClientMock and are never exercised by fetchSeriesFromStores.
type seriesStreamLifetimeClient struct {
	storeGatewayClientMock
	seriesFn func(ctx context.Context) (storegatewaypb.StoreGateway_SeriesClient, error)
}

func (c *seriesStreamLifetimeClient) Series(ctx context.Context, _ *storepb.SeriesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	return c.seriesFn(ctx)
}

// ctxAwareSeriesClient is a StoreGateway_SeriesClient that streams the queued responses
// (then io.EOF), but fails fast if its stream context is already cancelled. This lets a
// test detect a production change that cancels the per-stream context too early (before
// the response has been fully received).
type ctxAwareSeriesClient struct {
	grpc.ClientStream
	ctx       context.Context
	responses []*storepb.SeriesResponse
}

func (s *ctxAwareSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if len(s.responses) == 0 {
		return nil, io.EOF
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

// blockingSeriesClient is a StoreGateway_SeriesClient whose Recv blocks until either its
// stream context is cancelled or the test releases it. Blocking keeps one
// fetchSeriesFromStores goroutine (and thus the shared errgroup context) alive while a
// sibling request runs; honoring ctx cancellation lets a test assert that the stream is
// torn down when the errgroup context is cancelled. Only Recv is exercised by the code
// under test; the embedded nil grpc.ClientStream satisfies the rest of the interface.
type blockingSeriesClient struct {
	grpc.ClientStream
	ctx     context.Context
	started chan struct{}
	release chan struct{}
}

func (s *blockingSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case <-s.release:
		return nil, io.EOF
	}
}

// TestBlocksStoreQuerier_FetchSeriesFromStores_CancelsStreamPerRequest verifies that
// each store-gateway Series stream's context is cancelled as soon as its own fetch
// goroutine returns, instead of lingering until the shared errgroup context is cancelled
// when the slowest concurrent request finishes. See issue #7575.
func TestBlocksStoreQuerier_FetchSeriesFromStores_CancelsStreamPerRequest(t *testing.T) {
	minT, maxT := int64(10), int64(20)
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	// Fast store-gateway: capture the context handed to its Series stream, then return a
	// single hints frame followed by io.EOF so its goroutine returns quickly. The stream
	// is context-aware, so if the production code cancelled the context before the
	// response was received the fast request would fail (caught by require.NoError below).
	var fastStreamCtx context.Context
	fastCtxCaptured := make(chan struct{})
	fastClient := &seriesStreamLifetimeClient{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "fast"},
		seriesFn: func(ctx context.Context) (storegatewaypb.StoreGateway_SeriesClient, error) {
			fastStreamCtx = ctx
			close(fastCtxCaptured)
			return &ctxAwareSeriesClient{
				ctx:       ctx,
				responses: []*storepb.SeriesResponse{mockHintsResponse(block1)},
			}, nil
		},
	}

	// Slow store-gateway: block in Recv until released, keeping the errgroup alive.
	slowStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }
	defer doRelease()
	slowClient := &seriesStreamLifetimeClient{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "slow"},
		seriesFn: func(ctx context.Context) (storegatewaypb.StoreGateway_SeriesClient, error) {
			return &blockingSeriesClient{ctx: ctx, started: slowStarted, release: release}, nil
		},
	}

	q := &blocksStoreQuerier{}
	clients := map[BlocksStoreClient][]ulid.ULID{
		fastClient: {block1},
		slowClient: {block2},
	}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_metric")}

	// Capture the returned errors so we can assert the fast request took the success
	// path. This rules out a false pass where the fast goroutine errored and cancelled
	// the shared errgroup context (which would also fire fastStreamCtx.Done(), but for
	// the wrong reason).
	var gotErr, gotRetryErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _, gotErr, gotRetryErr = q.fetchSeriesFromStores(context.Background(), nil, "user-1", clients, minT, maxT, 0, matchers, 0, 0)
	}()

	// Wait until the slow request is blocked in Recv and the fast request's stream has
	// been created (so its goroutine is on its way to returning).
	select {
	case <-slowStarted:
	case <-time.After(5 * time.Second):
		doRelease()
		<-done
		t.Fatal("slow store-gateway Series stream never started")
	}
	select {
	case <-fastCtxCaptured:
	case <-time.After(5 * time.Second):
		doRelease()
		<-done
		t.Fatal("fast store-gateway Series stream was never created")
	}

	require.NotNil(t, fastStreamCtx)

	// The fast request has finished; its per-stream context must be cancelled even though
	// the slow request is still blocked in Recv. On the buggy code the fast stream shares
	// the errgroup context, which stays alive until the slow request returns, so this
	// never fires within the timeout.
	select {
	case <-fastStreamCtx.Done():
		// Expected: the fast stream's context was cancelled independently.
	case <-time.After(2 * time.Second):
		doRelease()
		<-done
		t.Fatal("fast store-gateway Series stream context was not cancelled while a concurrent request was still in-flight")
	}

	// Release the slow request and make sure fetchSeriesFromStores returns. Use t.Error
	// (not t.Fatal) so we don't runtime.Goexit the test goroutine while the worker may
	// still be running; release is already closed, so there is nothing left to unblock.
	doRelease()
	select {
	case <-done:
		// Both store-gateway requests succeeded, confirming the cancellation observed
		// above came from the fast request's own per-stream cancel, not from an error
		// cancelling the shared errgroup context.
		require.NoError(t, gotErr)
		require.NoError(t, gotRetryErr)
	case <-time.After(5 * time.Second):
		t.Error("fetchSeriesFromStores did not return after releasing the slow request")
	}
}

// TestBlocksStoreQuerier_FetchSeriesFromStores_SiblingErrorCancelsStreamContext verifies
// that each Series stream's context is derived from the errgroup context (gCtx): when one
// store-gateway request fails, the errgroup cancels gCtx, which must tear down the other,
// still-in-flight Series streams. A version that derived the per-stream context from a
// non-errgroup parent (e.g. the incoming ctx) would leave the blocked stream running and
// fetchSeriesFromStores would hang. See issue #7575.
func TestBlocksStoreQuerier_FetchSeriesFromStores_SiblingErrorCancelsStreamContext(t *testing.T) {
	minT, maxT := int64(10), int64(20)
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	// Blocked store-gateway: its Recv blocks until its stream context is cancelled (or the
	// deferred release fires as a safety net). The test never releases it on the happy
	// path — cancellation must come from the errgroup. It signals blockedStarted once it
	// is actually parked inside Recv.
	release := make(chan struct{})
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }
	defer doRelease()
	blockedStarted := make(chan struct{}, 1)
	blockedClient := &seriesStreamLifetimeClient{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "blocked"},
		seriesFn: func(ctx context.Context) (storegatewaypb.StoreGateway_SeriesClient, error) {
			return &blockingSeriesClient{ctx: ctx, started: blockedStarted, release: release}, nil
		},
	}

	// Failing store-gateway: waits until the blocked stream is parked inside Recv, then
	// returns a non-retryable error so the errgroup cancels gCtx (and therefore every
	// per-stream child context). Sequencing the error after the block guarantees the
	// blocked goroutine is committed to Recv (past the loop's gCtx.Err() guard), so the
	// only thing that can unblock it is cancellation of its own stream context — which
	// happens iff that context is derived from gCtx. This makes the wrong-parent
	// regression deterministic rather than scheduling-dependent.
	failingClient := &seriesStreamLifetimeClient{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "failing"},
		seriesFn: func(_ context.Context) (storegatewaypb.StoreGateway_SeriesClient, error) {
			select {
			case <-blockedStarted:
			case <-time.After(5 * time.Second):
			}
			return nil, status.Error(codes.Internal, "boom")
		},
	}

	q := &blocksStoreQuerier{}
	clients := map[BlocksStoreClient][]ulid.ULID{
		blockedClient: {block1},
		failingClient: {block2},
	}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_metric")}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _, _, _ = q.fetchSeriesFromStores(context.Background(), nil, "user-1", clients, minT, maxT, 0, matchers, 0, 0)
	}()

	// fetchSeriesFromStores must return promptly: the failing sibling cancels gCtx, which
	// cancels the blocked stream's context and unblocks its Recv. The test does not
	// release the blocked stream itself. On a version that derived the per-stream context
	// from a non-errgroup parent, the blocked Recv would never be cancelled and this hangs.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		doRelease()
		<-done
		t.Fatal("fetchSeriesFromStores did not return after a sibling store-gateway error; per-stream context is not derived from the errgroup context")
	}
}
