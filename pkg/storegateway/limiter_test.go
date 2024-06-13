package storegateway

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/store"
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
	l := &compositeLimiter{
		limiters: []store.BytesLimiter{
			store.NewLimiter(2, prometheus.NewCounter(prometheus.CounterOpts{})),
			store.NewLimiter(1, prometheus.NewCounter(prometheus.CounterOpts{})),
		},
	}

	assert.NoError(t, l.ReserveWithType(1, store.PostingsFetched))
	assert.Error(t, l.ReserveWithType(1, store.PostingsFetched))
}

func TestNewTokenBucketLimiter(t *testing.T) {
	podTokenBucket := util.NewTokenBucket(3, 3, nil)
	userTokenBucket := util.NewTokenBucket(2, 2, nil)
	requestTokenBucket := util.NewTokenBucket(1, 1, nil)
	l := newTokenBucketLimiter(podTokenBucket, userTokenBucket, requestTokenBucket, func(tokens uint64, dataType store.StoreDataType) int64 {
		if dataType == store.SeriesFetched {
			return int64(tokens) * 5
		}
		return int64(tokens)
	})

	// should force retrieve tokens from all buckets upon succeeding
	assert.NoError(t, l.ReserveWithType(2, store.PostingsFetched))
	assert.False(t, podTokenBucket.Retrieve(2))

	// should fail if user token bucket is running low
	podTokenBucket.Refund(3)
	userTokenBucket.Refund(2)
	requestTokenBucket.Refund(1)
	assert.ErrorContains(t, l.ReserveWithType(3, store.PostingsFetched), "not enough tokens in user token bucket")

	// should fail if pod token bucket is running low
	podTokenBucket.Refund(3)
	userTokenBucket.Refund(2)
	requestTokenBucket.Refund(1)
	podTokenBucket.ForceRetrieve(3)
	assert.ErrorContains(t, l.ReserveWithType(2, store.PostingsFetched), "not enough tokens in pod token bucket")

	// should retrieve different amount of tokens based on data type
	podTokenBucket.Refund(3)
	userTokenBucket.Refund(2)
	requestTokenBucket.Refund(1)
	assert.ErrorContains(t, l.ReserveWithType(1, store.SeriesFetched), "not enough tokens in user token bucket")

	// should always succeed if retrieve token bucket has enough tokens, although shared buckets are empty
	podTokenBucket.ForceRetrieve(3)
	userTokenBucket.ForceRetrieve(2)
	assert.NoError(t, l.ReserveWithType(1, store.PostingsFetched))
}
