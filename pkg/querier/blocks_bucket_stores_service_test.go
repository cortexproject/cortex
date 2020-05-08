package querier

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestBucketStoresService_InitialSync(t *testing.T) {
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
			cfg, cleanup := prepareStorageConfig(t)
			cfg.BucketStore.SyncInterval = testData.syncInterval
			defer cleanup()

			bucketClient := &tsdb.BucketClientMock{}
			testData.setup(bucketClient)

			us, err := NewBucketStoresService(cfg, bucketClient, mockLoggingLevel(), log.NewNopLogger(), nil)
			if err == nil {
				err = services.StartAndAwaitRunning(context.Background(), us)
				defer services.StopAndAwaitTerminated(context.Background(), us) //nolint:errcheck
			}

			if testData.expectedErr != nil && err != nil {
				require.Equal(t, testData.expectedErr.Error(), err.Error())
			} else {
				require.Equal(t, testData.expectedErr, err)
			}

			bucketClient.AssertNumberOfCalls(t, "Iter", testData.expectedIter)
		})
	}
}

func prepareStorageConfig(t *testing.T) (cortex_tsdb.Config, func()) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "blocks-sync-*")
	require.NoError(t, err)

	cfg := cortex_tsdb.Config{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = tmpDir

	cleanup := func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	}

	return cfg, cleanup
}

func mockLoggingLevel() logging.Level {
	level := logging.Level{}
	err := level.Set("info")
	if err != nil {
		panic(err)
	}

	return level
}
