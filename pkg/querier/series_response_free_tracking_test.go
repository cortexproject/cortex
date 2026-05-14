package querier

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/limiter"
)

type freeTracker struct {
	freed atomic.Bool
}

type singleFreePool struct {
	tracker *freeTracker
}

func (p *singleFreePool) Get(length int) *[]byte {
	b := make([]byte, length)
	return &b
}

func (p *singleFreePool) Put(_ *[]byte) {
	p.tracker.freed.Store(true)
}

func registerTrackingBuffer(resp *storepb.SeriesResponse, tracker *freeTracker) {
	data := make([]byte, 2048)
	resp.RegisterBuffer(mem.NewBuffer(&data, &singleFreePool{tracker: tracker}))
}

func TestFreeCalledOnAllResponses(t *testing.T) {
	t.Parallel()

	const (
		metricName = "test_metric"
		minT       = int64(10)
		maxT       = int64(20)
	)

	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	t.Run("success path", func(t *testing.T) {
		t.Parallel()

		resp1 := mockSeriesResponse(
			labels.FromStrings(labels.MetricName, metricName, "series", "1"),
			[]cortexpb.Sample{{Value: 1, TimestampMs: minT}}, nil, nil,
		)
		resp2 := mockSeriesResponse(
			labels.FromStrings(labels.MetricName, metricName, "series", "2"),
			[]cortexpb.Sample{{Value: 2, TimestampMs: minT}}, nil, nil,
		)
		resp3 := mockSeriesResponse(
			labels.FromStrings(labels.MetricName, metricName, "series", "3"),
			[]cortexpb.Sample{{Value: 3, TimestampMs: minT}}, nil, nil,
		)
		hintsResp := mockHintsResponse(block1, block2)

		tracker1 := &freeTracker{}
		tracker2 := &freeTracker{}
		tracker3 := &freeTracker{}
		trackerHints := &freeTracker{}
		registerTrackingBuffer(resp1, tracker1)
		registerTrackingBuffer(resp2, tracker2)
		registerTrackingBuffer(resp3, tracker3)
		registerTrackingBuffer(hintsResp, trackerHints)

		ctx := user.InjectOrgID(context.Background(), "user-1")
		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 0, 0, 0))

		stores := &blocksStoreSetMock{mockedResponses: []any{
			map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{
					remoteAddr:            "1.1.1.1",
					mockedSeriesResponses: []*storepb.SeriesResponse{resp1, resp2, resp3, hintsResp},
				}: {block1, block2},
			},
		}}

		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT, mock.Anything).
			Return(bucketindex.Blocks{
				&bucketindex.Block{ID: block1},
				&bucketindex.Block{ID: block2},
			}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		matchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
		}

		set := q.Select(ctx, true, &storage.SelectHints{Start: minT, End: maxT}, matchers...)
		require.NoError(t, set.Err())
		for set.Next() {
			_ = set.At()
		}

		assert.True(t, tracker1.freed.Load(), "resp1 should be freed")
		assert.True(t, tracker2.freed.Load(), "resp2 should be freed")
		assert.True(t, tracker3.freed.Load(), "resp3 should be freed")
		assert.True(t, trackerHints.freed.Load(), "hints resp should be freed")
	})

	t.Run("error path - series limit exceeded", func(t *testing.T) {
		t.Parallel()

		resp1 := mockSeriesResponse(
			labels.FromStrings(labels.MetricName, metricName, "series", "1"),
			[]cortexpb.Sample{{Value: 1, TimestampMs: minT}}, nil, nil,
		)
		resp2 := mockSeriesResponse(
			labels.FromStrings(labels.MetricName, metricName, "series", "2"),
			[]cortexpb.Sample{{Value: 2, TimestampMs: minT}}, nil, nil,
		)

		tracker1 := &freeTracker{}
		tracker2 := &freeTracker{}
		registerTrackingBuffer(resp1, tracker1)
		registerTrackingBuffer(resp2, tracker2)

		// Limit to 1 series so the second triggers an error.
		ctx := user.InjectOrgID(context.Background(), "user-1")
		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(1, 0, 0, 0))

		stores := &blocksStoreSetMock{mockedResponses: []any{
			map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{
					remoteAddr:            "1.1.1.1",
					mockedSeriesResponses: []*storepb.SeriesResponse{resp1, resp2},
				}: {block1, block2},
			},
		}}

		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT, mock.Anything).
			Return(bucketindex.Blocks{
				&bucketindex.Block{ID: block1},
				&bucketindex.Block{ID: block2},
			}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		matchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
		}

		set := q.Select(ctx, true, &storage.SelectHints{Start: minT, End: maxT}, matchers...)
		require.Error(t, set.Err())

		assert.True(t, tracker1.freed.Load(), "resp1 should be freed")
		assert.True(t, tracker2.freed.Load(), "resp2 should be freed (caused the error)")
	})
}
