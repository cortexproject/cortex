package storegateway

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/store"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestLimiter(t *testing.T) {
	l := &limiter{
		limiter: store.NewLimiter(2, prometheus.NewCounter(prometheus.CounterOpts{})),
	}

	assert.NoError(t, l.Reserve(1))
	assert.NoError(t, l.ReserveWithType(1, store.PostingsFetched))
	assert.Error(t, l.Reserve(1))
	assert.Error(t, l.ReserveWithType(1, store.PostingsFetched))
}

func TestCompositeLimiter(t *testing.T) {
	l := &compositeBytesLimiter{
		limiters: []store.BytesLimiter{
			&limiter{limiter: store.NewLimiter(2, prometheus.NewCounter(prometheus.CounterOpts{}))},
			&limiter{limiter: store.NewLimiter(1, prometheus.NewCounter(prometheus.CounterOpts{}))},
		},
	}

	assert.NoError(t, l.ReserveWithType(1, store.PostingsFetched))
	assert.ErrorContains(t, l.ReserveWithType(1, store.PostingsFetched), "(422)")
}

func TestNewTokenBucketBytesLimiter(t *testing.T) {
	tests := map[string]struct {
		tokensToRetrieve               []uint64
		requestTokenBucketSize         int64
		userTokenBucketSize            int64
		instanceTokenBucketSize        int64
		expectedRequestTokenRemaining  int64
		expectedUserTokenRemaining     int64
		expectedInstanceTokenRemaining int64
		getTokensToRetrieve            func(tokens uint64, dataType store.StoreDataType) int64
		errCode                        int
		dryRun                         bool
	}{
		"should retrieve buckets from all buckets": {
			tokensToRetrieve:        []uint64{1},
			requestTokenBucketSize:  1,
			userTokenBucketSize:     1,
			instanceTokenBucketSize: 1,
		},
		"should succeed if there is enough request token, regardless of user or instance bucket": {
			tokensToRetrieve:               []uint64{1},
			requestTokenBucketSize:         1,
			userTokenBucketSize:            0,
			instanceTokenBucketSize:        0,
			expectedUserTokenRemaining:     -1,
			expectedInstanceTokenRemaining: -1,
		},
		"should throw 429 if not enough user tokens remaining": {
			tokensToRetrieve:              []uint64{1, 1},
			requestTokenBucketSize:        1,
			userTokenBucketSize:           1,
			instanceTokenBucketSize:       2,
			errCode:                       429,
			expectedRequestTokenRemaining: -1,
			expectedUserTokenRemaining:    -1,
		},
		"should throw 422 if request is greater than user token bucket size": {
			tokensToRetrieve:              []uint64{2},
			requestTokenBucketSize:        1,
			userTokenBucketSize:           1,
			instanceTokenBucketSize:       2,
			errCode:                       422,
			expectedRequestTokenRemaining: -1,
			expectedUserTokenRemaining:    -1,
		},
		"should throw 429 if not enough instance tokesn remaining": {
			tokensToRetrieve:               []uint64{1, 1},
			requestTokenBucketSize:         1,
			userTokenBucketSize:            2,
			instanceTokenBucketSize:        1,
			errCode:                        429,
			expectedRequestTokenRemaining:  -1,
			expectedInstanceTokenRemaining: -1,
		},
		"should throw 422 if request is greater than instance token bucket size": {
			tokensToRetrieve:               []uint64{2},
			requestTokenBucketSize:         1,
			userTokenBucketSize:            2,
			instanceTokenBucketSize:        1,
			errCode:                        422,
			expectedRequestTokenRemaining:  -1,
			expectedInstanceTokenRemaining: -1,
		},
		"should use getTokensToRetrieve to calculate tokens": {
			tokensToRetrieve: []uint64{1},
			getTokensToRetrieve: func(tokens uint64, dataType store.StoreDataType) int64 {
				if dataType == store.PostingsFetched {
					return 0
				}
				return 1
			},
		},
		"should not fail if dryRun": {
			tokensToRetrieve:               []uint64{1},
			expectedRequestTokenRemaining:  -1,
			expectedUserTokenRemaining:     -1,
			expectedInstanceTokenRemaining: -1,
			dryRun:                         true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			requestTokenBucket := util.NewTokenBucket(testData.requestTokenBucketSize, nil)
			userTokenBucket := util.NewTokenBucket(testData.userTokenBucketSize, nil)
			instanceTokenBucket := util.NewTokenBucket(testData.instanceTokenBucketSize, nil)

			getTokensToRetrieve := func(tokens uint64, dataType store.StoreDataType) int64 {
				return int64(tokens)
			}
			if testData.getTokensToRetrieve != nil {
				getTokensToRetrieve = testData.getTokensToRetrieve
			}
			l := newTokenBucketBytesLimiter(requestTokenBucket, userTokenBucket, instanceTokenBucket, testData.dryRun, prometheus.NewCounter(prometheus.CounterOpts{}), getTokensToRetrieve)

			var err error
			for _, token := range testData.tokensToRetrieve {
				err = l.ReserveWithType(token, store.PostingsFetched)
			}

			assert.Equal(t, testData.expectedRequestTokenRemaining, requestTokenBucket.Retrieve(0))
			assert.Equal(t, testData.expectedUserTokenRemaining, userTokenBucket.Retrieve(0))
			assert.Equal(t, testData.expectedInstanceTokenRemaining, instanceTokenBucket.Retrieve(0))

			if testData.errCode > 0 {
				assert.ErrorContains(t, err, fmt.Sprintf("(%d)", testData.errCode))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
