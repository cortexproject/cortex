package frontend

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func setupFrontend(t *testing.T, config Config) *Frontend {
	logger := log.NewNopLogger()

	frontend, err := New(config, logger)
	require.NoError(t, err)
	defer frontend.Close()
	return frontend
}

func testReq(ctx context.Context) *request {
	return &request{
		originalCtx: ctx,
		err:         make(chan error, 1),
		response:    make(chan *ProcessResponse, 1),
	}
}

func TestDequeuesExpiredRequests(t *testing.T) {
	var config Config
	flagext.DefaultValues(&config)
	config.MaxOutstandingPerTenant = 10
	userID := "1"
	userID2 := "2"

	f := setupFrontend(t, config)

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

	// the first request shouldnt be expired
	req, err := f.getNextRequest(ctx)
	require.Nil(t, err)
	require.NotNil(t, req)
	require.Equal(t, 9, len(f.queues[userID]))

	// the next unexpired request should be the 5th index
	req, err = f.getNextRequest(ctx)
	require.Nil(t, err)
	require.NotNil(t, req)
	require.Equal(t, 4, len(f.queues[userID]))

	// add one request to a second tenant queue
	ctx2 := user.InjectOrgID(context.Background(), userID2)
	err = f.queueRequest(ctx2, testReq(ctx2))
	require.Nil(t, err)

	// there should be no more unexpired requests in queue until the second tenant enqueues one.
	req, err = f.getNextRequest(ctx)
	require.Nil(t, err)
	require.NotNil(t, req)

	// ensure either one or two queues are fully drained, depending on which was requested first
	_, ok := f.queues[userID]
	if ok {
		// if the second user's queue was chosen for the last request,
		// the first queue should still contain 4 (expired) requests.
		require.Equal(t, 4, len(f.queues[userID]))
	}
	_, ok = f.queues[userID2]
	require.Equal(t, false, ok)
}
