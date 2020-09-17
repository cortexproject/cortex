package frontend

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func setupFrontend(config Config) (*Frontend, error) {
	logger := log.NewNopLogger()

	frontend, err := New(config, limits{queriers: 3}, logger, nil)
	if err != nil {
		return nil, err
	}

	defer frontend.Close()
	return frontend, nil
}

func testReq(ctx context.Context) *request {
	return &request{
		originalCtx: ctx,
		err:         make(chan error, 1),
		response:    make(chan *httpgrpc.HTTPResponse, 1),
	}
}

func TestDequeuesExpiredRequests(t *testing.T) {
	var config Config
	flagext.DefaultValues(&config)
	config.MaxOutstandingPerTenant = 10
	userID := "1"
	userID2 := "2"

	f, err := setupFrontend(config)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), userID)
	expired, cancel := context.WithCancel(ctx)
	cancel()

	for i := 0; i < config.MaxOutstandingPerTenant; i++ {
		var err error
		if i%5 == 0 {
			err = f.queueRequest(ctx, testReq(ctx))
		} else {
			err = f.queueRequest(ctx, testReq(expired))
		}

		require.Nil(t, err)
	}

	// the first request shouldn't be expired
	req, idx, err := f.getNextRequestForQuerier(ctx, -1, "")
	require.Nil(t, err)
	require.NotNil(t, req)
	require.Equal(t, 9, len(f.queues.getOrAddQueue(userID, 0)))

	// the next unexpired request should be the 5th index
	req, idx, err = f.getNextRequestForQuerier(ctx, idx, "")
	require.Nil(t, err)
	require.NotNil(t, req)
	require.Equal(t, 4, len(f.queues.getOrAddQueue(userID, 0)))

	// add one request to a second tenant queue
	ctx2 := user.InjectOrgID(context.Background(), userID2)
	err = f.queueRequest(ctx2, testReq(ctx2))
	require.Nil(t, err)

	// there should be no more unexpired requests in queue until the second tenant enqueues one.
	req, _, err = f.getNextRequestForQuerier(ctx, idx, "")
	require.Nil(t, err)
	require.NotNil(t, req)

	// ensure either one or two queues are fully drained, depending on which was requested first
	_, ok := f.queues.userQueues[userID]
	if ok {
		// if the second user's queue was chosen for the last request,
		// the first queue should still contain 4 (expired) requests.
		require.Equal(t, 4, len(f.queues.getOrAddQueue(userID, 0)))
	}
	_, ok = f.queues.userQueues[userID2]
	require.Equal(t, false, ok)
}

func TestRoundRobinQueues(t *testing.T) {
	var config Config
	flagext.DefaultValues(&config)
	config.MaxOutstandingPerTenant = 100

	f, err := setupFrontend(config)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		userID := fmt.Sprint(i / 10)
		ctx := user.InjectOrgID(context.Background(), userID)

		err = f.queueRequest(ctx, testReq(ctx))
		require.NoError(t, err)
	}

	ctx := context.Background()
	idx := -1
	for i := 0; i < 100; i++ {
		req, nidx, err := f.getNextRequestForQuerier(ctx, idx, "")
		require.NoError(t, err)
		require.NotNil(t, req)
		idx = nidx

		userID, err := user.ExtractOrgID(req.originalCtx)
		require.NoError(t, err)
		intUserID, err := strconv.Atoi(userID)
		require.NoError(t, err)

		require.Equal(t, i%10, intUserID)
	}
}

func BenchmarkGetNextRequest(b *testing.B) {
	var config Config
	flagext.DefaultValues(&config)
	config.MaxOutstandingPerTenant = 2

	const numTenants = 50
	const queriers = 5

	frontends := make([]*Frontend, 0, b.N)

	for n := 0; n < b.N; n++ {
		f, err := setupFrontend(config)
		if err != nil {
			b.Fatal(err)
		}

		for ix := 0; ix < queriers; ix++ {
			f.registerQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		for i := 0; i < config.MaxOutstandingPerTenant; i++ {
			for j := 0; j < numTenants; j++ {
				userID := strconv.Itoa(j)
				ctx := user.InjectOrgID(context.Background(), userID)

				err = f.queueRequest(ctx, testReq(ctx))
				if err != nil {
					b.Fatal(err)
				}
			}
		}

		frontends = append(frontends, f)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := -1
		for j := 0; j < config.MaxOutstandingPerTenant*numTenants; j++ {
			querier := ""
		b:
			// Find querier with at least one request to avoid blocking in getNextRequestForQuerier.
			for _, q := range frontends[i].queues.userQueues {
				for qid := range q.queriers {
					querier = qid
					break b
				}
			}

			_, nidx, err := frontends[i].getNextRequestForQuerier(ctx, idx, querier)
			if err != nil {
				b.Fatal(err)
			}
			idx = nidx
		}
	}
}

func BenchmarkQueueRequest(b *testing.B) {
	var config Config
	flagext.DefaultValues(&config)
	config.MaxOutstandingPerTenant = 2

	const numTenants = 50
	const queriers = 5

	frontends := make([]*Frontend, 0, b.N)
	contexts := make([]context.Context, 0, numTenants)
	requests := make([]*request, 0, numTenants)

	for n := 0; n < b.N; n++ {
		f, err := setupFrontend(config)
		if err != nil {
			b.Fatal(err)
		}

		for ix := 0; ix < queriers; ix++ {
			f.registerQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		frontends = append(frontends, f)

		for j := 0; j < numTenants; j++ {
			userID := strconv.Itoa(j)
			ctx := user.InjectOrgID(context.Background(), userID)
			r := testReq(ctx)

			requests = append(requests, r)
			contexts = append(contexts, ctx)
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < config.MaxOutstandingPerTenant; i++ {
			for j := 0; j < numTenants; j++ {
				err := frontends[n].queueRequest(contexts[j], requests[j])
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}
