package querier

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestUserStore_InitialSync(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setup        func(bucketClient *tsdb.BucketClientMock)
		syncInterval time.Duration
		expectedIter int
		expectedErr  error
	}{
		"should sync blocks for all tenants": {
			setup: func(bucketClient *tsdb.BucketClientMock) {
				bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
				bucketClient.MockIter("user-1/", []string{}, nil)
				bucketClient.MockIter("user-2/", []string{}, nil)
			},
			syncInterval: time.Minute,
			expectedIter: 3,
		},
		"should not sync blocks if sync interval is 0": {
			setup: func(bucketClient *tsdb.BucketClientMock) {
				bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
				bucketClient.MockIter("user-1/", []string{}, nil)
				bucketClient.MockIter("user-2/", []string{}, nil)
			},
			syncInterval: 0,
			expectedIter: 0,
		},
		"should return error on initial sync failed": {
			setup: func(bucketClient *tsdb.BucketClientMock) {
				bucketClient.MockIter("", nil, errors.New("mocked error"))
			},
			syncInterval: time.Minute,
			expectedIter: 1,
			expectedErr:  errors.New("mocked error"),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := tsdb.Config{}
			flagext.DefaultValues(&cfg)
			cfg.BucketStore.SyncInterval = testData.syncInterval

			bucketClient := &tsdb.BucketClientMock{}
			testData.setup(bucketClient)

			us, err := NewUserStore(cfg, bucketClient, mockLoggingLevel(), log.NewNopLogger(), nil)
			if err == nil {
				err = services.StartAndAwaitRunning(context.Background(), us)
				defer services.StopAndAwaitTerminated(context.Background(), us) //nolint:errcheck
			}

			require.Equal(t, testData.expectedErr, err)
			bucketClient.AssertNumberOfCalls(t, "Iter", testData.expectedIter)
		})
	}
}

func TestUserStore_syncUserStores(t *testing.T) {
	cfg := tsdb.Config{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.TenantSyncConcurrency = 2

	// Disable the sync interval so that there will be no initial sync.
	cfg.BucketStore.SyncInterval = 0

	bucketClient := &tsdb.BucketClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2", "user-3"}, nil)

	us, err := NewUserStore(cfg, bucketClient, mockLoggingLevel(), log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), us))
	defer services.StopAndAwaitTerminated(context.Background(), us) //nolint:errcheck

	// Sync user stores and count the number of times the callback is called.
	storesCount := int32(0)
	err = us.syncUserStores(context.Background(), func(ctx context.Context, bs *store.BucketStore) error {
		atomic.AddInt32(&storesCount, 1)
		return nil
	})

	assert.NoError(t, err)
	bucketClient.AssertNumberOfCalls(t, "Iter", 1)
	assert.Equal(t, storesCount, int32(3))
}

func mockLoggingLevel() logging.Level {
	level := logging.Level{}
	err := level.Set("info")
	if err != nil {
		panic(err)
	}

	return level
}
